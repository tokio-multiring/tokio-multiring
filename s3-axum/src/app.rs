//! Unified Axum S3 router and handlers that run identically on BOTH:
//! - `tokio-sidecar` + `sidecar-axum` (`io_uring` + `ureq-proto`, zero-copy &
//!   registered buffers via `DirectBody` / `DirectResponse`), and
//! - standard Tokio + `axum::serve` (Hyper HTTP/1.1 + `spawn_blocking` I/O).

use std::ops::Range;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, put};
use http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header};
use sidecar_axum::{DirectBody, DirectResponse};

use crate::date;
use crate::error::{ApiError, StoreError};
use crate::name::{Bucket, Object, Target};
use crate::sidecar_store::{SidecarStore, SidecarStored};
use crate::stripe::Meta;
use crate::tokio_store::{TokioStore, TokioStored};

/// Storage backend powering the unified Axum S3 app.
#[derive(Clone)]
pub enum Backend {
    Sidecar(Arc<SidecarStore>),
    Tokio(Arc<TokioStore>),
}

/// Builds the unified Axum S3 [`Router`] for either runtime.
pub fn router(backend: Backend) -> Router {
    Router::new()
        .route("/", any(service_root))
        .route(
            "/{bucket}",
            put(create_bucket)
                .head(head_bucket)
                .delete(delete_bucket)
                .get(not_implemented)
                .fallback(method_not_allowed),
        )
        .route(
            "/{bucket}/",
            put(create_bucket)
                .head(head_bucket)
                .delete(delete_bucket)
                .get(not_implemented)
                .fallback(method_not_allowed),
        )
        .route(
            "/{bucket}/{*key}",
            put(put_object)
                .get(get_object)
                .head(head_object)
                .delete(delete_object)
                .fallback(method_not_allowed),
        )
        .fallback(fallback_handler)
        .with_state(backend)
}

fn check_no_query(uri: &Uri) -> Result<(), ApiError> {
    if uri.query().is_some_and(|q| !q.is_empty()) {
        return Err(ApiError::NOT_IMPLEMENTED);
    }
    Ok(())
}

fn parse_bucket(uri: &Uri) -> Result<Bucket, ApiError> {
    check_no_query(uri)?;
    match Target::parse(uri.path())? {
        Target::Bucket(b) => Ok(b),
        Target::Service | Target::Object(_) => Err(ApiError::INVALID_BUCKET_NAME),
    }
}

fn parse_object(uri: &Uri) -> Result<Object, ApiError> {
    check_no_query(uri)?;
    match Target::parse(uri.path())? {
        Target::Object(o) => Ok(o),
        Target::Service | Target::Bucket(_) => Err(ApiError::INVALID_URI),
    }
}

fn entity_tag(stat: &Meta) -> String {
    let (seconds, nanoseconds) = stat.modified;
    format!("\"{seconds:x}{nanoseconds:08x}-{:x}\"", stat.size)
}

fn empty_response(status: StatusCode) -> Response {
    let mut builder = Response::builder()
        .status(status)
        .header(header::DATE, date::now());
    if status != StatusCode::NO_CONTENT {
        builder = builder.header(header::CONTENT_LENGTH, 0);
    }
    builder.body(Body::empty()).expect("valid empty response")
}

async fn service_root(method: Method, uri: Uri) -> ApiError {
    if check_no_query(&uri).is_err() {
        return ApiError::NOT_IMPLEMENTED;
    }
    if method == Method::GET {
        ApiError::NOT_IMPLEMENTED
    } else {
        ApiError::METHOD_NOT_ALLOWED
    }
}

async fn not_implemented() -> ApiError {
    ApiError::NOT_IMPLEMENTED
}

async fn method_not_allowed() -> ApiError {
    ApiError::METHOD_NOT_ALLOWED
}

async fn fallback_handler(uri: Uri) -> ApiError {
    match Target::parse(uri.path()) {
        Err(err) => err,
        Ok(_) => ApiError::METHOD_NOT_ALLOWED,
    }
}

async fn create_bucket(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    let bucket = parse_bucket(&uri)?;
    match &backend {
        Backend::Sidecar(s) => s.create_bucket(&bucket).await?,
        Backend::Tokio(s) => s.create_bucket(&bucket).await?,
    }
    Ok(empty_response(StatusCode::OK))
}

async fn head_bucket(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    let bucket = parse_bucket(&uri)?;
    match &backend {
        Backend::Sidecar(s) => s.head_bucket(&bucket).await?,
        Backend::Tokio(s) => s.head_bucket(&bucket).await?,
    }
    Ok(empty_response(StatusCode::OK))
}

async fn delete_bucket(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    let bucket = parse_bucket(&uri)?;
    match &backend {
        Backend::Sidecar(s) => s.delete_bucket(&bucket).await?,
        Backend::Tokio(s) => s.delete_bucket(&bucket).await?,
    }
    Ok(empty_response(StatusCode::NO_CONTENT))
}

async fn put_object(
    State(backend): State<Backend>,
    uri: Uri,
    body: DirectBody,
) -> Result<Response, StoreError> {
    let object = parse_object(&uri)?;
    let meta = match &backend {
        Backend::Sidecar(s) => s.put_object(&object, body).await?,
        Backend::Tokio(s) => s.put_object(&object, body).await?,
    };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::DATE, date::now())
        .header(header::ETAG, entity_tag(&meta))
        .header(header::CONTENT_LENGTH, 0)
        .body(Body::empty())
        .expect("valid put response"))
}

async fn head_object(
    State(backend): State<Backend>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    let object = parse_object(&uri)?;
    let meta = match &backend {
        Backend::Sidecar(s) => s.head_object(&object).await?,
        Backend::Tokio(s) => s.head_object(&object).await?,
    };
    Ok(build_object_response(
        meta,
        headers.get(header::RANGE),
        ObjectPayload::HeadOnly,
    ))
}

async fn get_object(
    State(backend): State<Backend>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    let object = parse_object(&uri)?;
    match &backend {
        Backend::Sidecar(s) => {
            let stored = s.open_object(&object).await?;
            let meta = stored.meta;
            Ok(build_object_response(
                meta,
                headers.get(header::RANGE),
                ObjectPayload::Sidecar(s.clone(), stored),
            ))
        }
        Backend::Tokio(s) => {
            let stored = s.open_object(&object).await?;
            let meta = stored.meta;
            Ok(build_object_response(
                meta,
                headers.get(header::RANGE),
                ObjectPayload::Tokio(s.clone(), stored),
            ))
        }
    }
}

async fn delete_object(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    let object = parse_object(&uri)?;
    match &backend {
        Backend::Sidecar(s) => s.delete_object(&object).await?,
        Backend::Tokio(s) => s.delete_object(&object).await?,
    }
    Ok(empty_response(StatusCode::NO_CONTENT))
}

enum ObjectPayload {
    HeadOnly,
    Sidecar(Arc<SidecarStore>, SidecarStored),
    Tokio(Arc<TokioStore>, TokioStored),
}

fn build_object_response(
    stat: Meta,
    range_header: Option<&HeaderValue>,
    payload: ObjectPayload,
) -> Response {
    let (status, range) = match Requested::parse(range_header, stat.size) {
        Requested::Whole => (StatusCode::OK, 0..stat.size),
        Requested::Part(range) => (StatusCode::PARTIAL_CONTENT, range),
        Requested::Unsatisfiable => {
            let total = HeaderValue::try_from(format!("bytes */{}", stat.size))
                .expect("digits make a valid header value");
            return ApiError::INVALID_RANGE.into_response_with_header(header::CONTENT_RANGE, total);
        }
    };

    let mut builder = Response::builder()
        .status(status)
        .header(header::DATE, date::now())
        .header(header::CONTENT_TYPE, "binary/octet-stream")
        .header(header::CONTENT_LENGTH, range.end - range.start)
        .header(header::ETAG, entity_tag(&stat))
        .header(header::LAST_MODIFIED, date::http_date(stat.modified.0))
        .header(header::ACCEPT_RANGES, "bytes");
    if status == StatusCode::PARTIAL_CONTENT {
        let part = format!("bytes {}-{}/{}", range.start, range.end - 1, stat.size);
        builder = builder.header(header::CONTENT_RANGE, part);
    }

    match payload {
        ObjectPayload::HeadOnly => builder.body(Body::empty()).expect("valid head response"),
        ObjectPayload::Sidecar(store, stored) => {
            let head = builder.body(()).expect("valid response head");
            DirectResponse::new(head, store.writer(stored, range)).into_response()
        }
        ObjectPayload::Tokio(store, stored) => {
            let body = store.stream_body(stored, range);
            builder.body(body).expect("valid stream response")
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Requested {
    Whole,
    Part(Range<u64>),
    Unsatisfiable,
}

impl Requested {
    fn parse(header: Option<&HeaderValue>, size: u64) -> Requested {
        let spec = header
            .and_then(|header| header.to_str().ok())
            .and_then(|header| header.strip_prefix("bytes="));
        let Some((first, last)) = spec.and_then(|spec| spec.split_once('-')) else {
            return Requested::Whole;
        };

        if first.is_empty() {
            return match last.parse::<u64>() {
                Ok(0) => Requested::Unsatisfiable,
                Ok(_) if size == 0 => Requested::Unsatisfiable,
                Ok(count) => Requested::Part(size.saturating_sub(count)..size),
                Err(_) => Requested::Whole,
            };
        }
        let Ok(start) = first.parse::<u64>() else {
            return Requested::Whole;
        };
        let end = if last.is_empty() {
            size
        } else {
            match last.parse::<u64>() {
                Ok(last) if last >= start => last.saturating_add(1).min(size),
                _ => return Requested::Whole,
            }
        };
        if start >= size {
            return Requested::Unsatisfiable;
        }
        Requested::Part(start..end)
    }
}
