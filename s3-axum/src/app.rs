//! Unified Axum S3 router, handlers, and middleware running identically on BOTH:
//! - `tokio-multiring` + `multiring-axum` (`io_uring` + `ureq-proto`, zero-copy &
//!   registered buffers via `DirectBody` / `DirectResponse`), and
//! - standard Tokio + `axum::serve` (Hyper HTTP/1.1 + `spawn_blocking` I/O).

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::extract::{Query, Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, header};
use multiring_axum::{DirectBody, DirectResponse};
use serde::Deserialize;

use crate::date;
use crate::error::{ApiError, StoreError};
use crate::multiring_store::{MultiRingStore, MultiRingStored};
use crate::name::{Bucket, Object, Target};
use crate::sigv4::{self, AuthConfig, PayloadMode};
use crate::stripe::Meta;
use crate::tokio_store::{TokioStore, TokioStored};
use crate::tokio_uring_store::{TokioUringStore, TokioUringStored};

static REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static X_AMZ_REQUEST_ID: HeaderName = HeaderName::from_static("x-amz-request-id");
static X_S3_RUNTIME: HeaderName = HeaderName::from_static("x-s3-runtime");

/// Storage backend powering the unified Axum S3 app.
#[derive(Clone)]
pub enum Backend {
    MultiRing(Arc<MultiRingStore>),
    Tokio(Arc<TokioStore>),
    TokioUring(Arc<TokioUringStore>),
}

impl Backend {
    fn runtime_header(&self) -> HeaderValue {
        match self {
            Backend::MultiRing(_) => HeaderValue::from_static("multiring"),
            Backend::Tokio(_) => HeaderValue::from_static("tokio"),
            Backend::TokioUring(_) => HeaderValue::from_static("tokio-uring"),
        }
    }
}

/// Optional router-level middleware settings.
#[derive(Clone, Debug, Default)]
pub struct AppOptions {
    /// Log method, path, status, and latency for every request via Axum middleware.
    pub trace: bool,
    /// Optional AWS SigV4 authentication credentials. When `Some`, every request
    /// must carry a valid `AWS4-HMAC-SHA256` `Authorization` header.
    pub auth: Option<AuthConfig>,
}

/// Builds the unified Axum S3 [`Router`] for either runtime with default options.
pub fn router(backend: Backend) -> Router {
    router_with_options(backend, AppOptions::default())
}

/// Builds the unified Axum S3 [`Router`] with explicit [`AppOptions`].
pub fn router_with_options(backend: Backend, options: AppOptions) -> Router {
    let runtime_header = backend.runtime_header();
    let app = Router::new()
        .route("/", get(list_buckets).fallback(method_not_allowed))
        .route(
            "/{bucket}",
            put(create_bucket)
                .head(head_bucket)
                .delete(delete_bucket)
                .get(list_objects)
                .fallback(method_not_allowed),
        )
        .route(
            "/{bucket}/",
            put(create_bucket)
                .head(head_bucket)
                .delete(delete_bucket)
                .get(list_objects)
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
        .with_state(backend);

    // Optional SigV4 authentication layer (runs before the handler while
    // leaving DirectBody / DirectResponse completely intact).
    let app = if let Some(auth) = options.auth {
        let auth = Arc::new(auth);
        app.layer(middleware::from_fn(move |mut req: Request, next: Next| {
            let auth = auth.clone();
            async move {
                match sigv4::verify_request(&auth, req.method(), req.uri(), req.headers()) {
                    Ok(mode) => {
                        req.extensions_mut().insert(mode);
                        next.run(req).await
                    }
                    Err(err) => err.into_response(),
                }
            }
        }))
    } else {
        app
    };

    // Real Axum middleware layer that runs on every request on BOTH runtimes
    // while leaving `DirectBody` and `DirectResponse` intact on multiring-axum.
    let trace = options.trace;
    app.layer(middleware::from_fn(move |req: Request, next: Next| {
        let runtime_header = runtime_header.clone();
        async move {
            let started = trace.then(Instant::now);
            let method = trace.then(|| req.method().clone());
            let path = trace.then(|| req.uri().path().to_owned());

            let mut resp = next.run(req).await;

            let id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
            let hex = format_hex_u64(id);
            let req_id =
                HeaderValue::from_bytes(&hex).expect("hex ASCII bytes are a valid header value");
            resp.headers_mut().insert(X_AMZ_REQUEST_ID.clone(), req_id);
            resp.headers_mut()
                .insert(X_S3_RUNTIME.clone(), runtime_header);

            if let (Some(started), Some(method), Some(path)) = (started, method, path) {
                log::info!(
                    "{method} {path} -> {} ({:.2?})",
                    resp.status().as_u16(),
                    started.elapsed()
                );
            }
            resp
        }
    }))
}

fn format_hex_u64(mut value: u64) -> [u8; 16] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = [b'0'; 16];
    for slot in out.iter_mut().rev() {
        *slot = HEX[(value & 0xf) as usize];
        value >>= 4;
    }
    out
}

fn check_no_query(uri: &Uri) -> Result<(), ApiError> {
    if uri.query().is_some_and(|q| !q.is_empty()) {
        return Err(ApiError::NOT_IMPLEMENTED);
    }
    Ok(())
}

fn parse_bucket_path(uri: &Uri) -> Result<Bucket, ApiError> {
    match Target::parse(uri.path())? {
        Target::Bucket(b) => Ok(b),
        Target::Service | Target::Object(_) => Err(ApiError::INVALID_BUCKET_NAME),
    }
}

fn parse_bucket(uri: &Uri) -> Result<Bucket, ApiError> {
    check_no_query(uri)?;
    parse_bucket_path(uri)
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

fn xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

fn xml_response(xml: String) -> Response {
    let bytes = xml.into_bytes();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::DATE, date::now())
        .header(header::CONTENT_TYPE, "application/xml")
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(Body::from(bytes))
        .expect("valid XML response")
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

async fn method_not_allowed() -> ApiError {
    ApiError::METHOD_NOT_ALLOWED
}

async fn fallback_handler(uri: Uri) -> ApiError {
    match Target::parse(uri.path()) {
        Err(err) => err,
        Ok(_) => ApiError::METHOD_NOT_ALLOWED,
    }
}

/// `GET /` — S3 `ListBuckets`.
async fn list_buckets(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    check_no_query(&uri)?;
    let buckets = match &backend {
        Backend::MultiRing(s) => s.list_buckets().await?,
        Backend::Tokio(s) => s.list_buckets().await?,
        Backend::TokioUring(s) => s.list_buckets().await?,
    };
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Buckets>",
    );
    for bucket in buckets {
        xml.push_str("<Bucket><Name>");
        xml.push_str(&xml_escape(&bucket.name));
        xml.push_str("</Name><CreationDate>");
        xml.push_str(&date::iso8601(bucket.modified_secs));
        xml.push_str("</CreationDate></Bucket>");
    }
    xml.push_str("</Buckets></ListAllMyBucketsResult>");
    Ok(xml_response(xml))
}

#[derive(Debug, Default, Deserialize)]
struct ListObjectsQuery {
    #[serde(rename = "list-type")]
    list_type: Option<String>,
    prefix: Option<String>,
    #[serde(rename = "max-keys")]
    max_keys: Option<usize>,
    #[serde(rename = "start-after")]
    start_after: Option<String>,
    #[serde(rename = "continuation-token")]
    continuation_token: Option<String>,
}

/// `GET /{bucket}` — S3 `ListObjectsV2` (and v1 fallback when `list-type` is omitted).
async fn list_objects(
    State(backend): State<Backend>,
    uri: Uri,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Response, StoreError> {
    let bucket = parse_bucket_path(&uri)?;
    if let Some(ref lt) = query.list_type
        && lt != "2"
    {
        return Err(ApiError::NOT_IMPLEMENTED.into());
    }
    let prefix = query.prefix.as_deref().unwrap_or("");
    let max_keys = query.max_keys.unwrap_or(1000).min(1000);
    let after = query
        .continuation_token
        .as_deref()
        .or(query.start_after.as_deref());

    let page = match &backend {
        Backend::MultiRing(s) => s.list_objects(&bucket, prefix, after, max_keys).await?,
        Backend::Tokio(s) => s.list_objects(&bucket, prefix, after, max_keys).await?,
        Backend::TokioUring(s) => s.list_objects(&bucket, prefix, after, max_keys).await?,
    };

    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>{}</MaxKeys>\
         <IsTruncated>{}</IsTruncated>",
        xml_escape(bucket.as_str()),
        xml_escape(prefix),
        page.contents.len(),
        max_keys,
        page.is_truncated,
    );
    if let Some(ref token) = page.next_continuation_token {
        xml.push_str("<NextContinuationToken>");
        xml.push_str(&xml_escape(token));
        xml.push_str("</NextContinuationToken>");
    }
    for obj in &page.contents {
        xml.push_str("<Contents><Key>");
        xml.push_str(&xml_escape(&obj.key));
        xml.push_str("</Key><LastModified>");
        xml.push_str(&date::iso8601(obj.meta.modified.0));
        xml.push_str("</LastModified><ETag>");
        xml.push_str(&xml_escape(&entity_tag(&obj.meta)));
        xml.push_str("</ETag><Size>");
        xml.push_str(&obj.meta.size.to_string());
        xml.push_str("</Size></Contents>");
    }
    xml.push_str("</ListBucketResult>");
    Ok(xml_response(xml))
}

async fn create_bucket(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    let bucket = parse_bucket(&uri)?;
    match &backend {
        Backend::MultiRing(s) => s.create_bucket(&bucket).await?,
        Backend::Tokio(s) => s.create_bucket(&bucket).await?,
        Backend::TokioUring(s) => s.create_bucket(&bucket).await?,
    }
    Ok(empty_response(StatusCode::OK))
}

async fn head_bucket(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    let bucket = parse_bucket(&uri)?;
    match &backend {
        Backend::MultiRing(s) => s.head_bucket(&bucket).await?,
        Backend::Tokio(s) => s.head_bucket(&bucket).await?,
        Backend::TokioUring(s) => s.head_bucket(&bucket).await?,
    }
    Ok(empty_response(StatusCode::OK))
}

async fn delete_bucket(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    let bucket = parse_bucket(&uri)?;
    match &backend {
        Backend::MultiRing(s) => s.delete_bucket(&bucket).await?,
        Backend::Tokio(s) => s.delete_bucket(&bucket).await?,
        Backend::TokioUring(s) => s.delete_bucket(&bucket).await?,
    }
    Ok(empty_response(StatusCode::NO_CONTENT))
}

async fn put_object(
    State(backend): State<Backend>,
    uri: Uri,
    headers: HeaderMap,
    extensions: http::Extensions,
    body: DirectBody,
) -> Result<Response, StoreError> {
    let object = parse_object(&uri)?;
    let mode = extensions
        .get::<PayloadMode>()
        .cloned()
        .unwrap_or_else(|| sigv4::detect_unauthenticated_mode(&headers));
    let meta = match &backend {
        Backend::MultiRing(s) => s.put_object(&object, body, mode).await?,
        Backend::Tokio(s) => s.put_object(&object, body, mode).await?,
        Backend::TokioUring(s) => s.put_object(&object, body, mode).await?,
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
        Backend::MultiRing(s) => s.head_object(&object).await?,
        Backend::Tokio(s) => s.head_object(&object).await?,
        Backend::TokioUring(s) => s.head_object(&object).await?,
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
        Backend::MultiRing(s) => {
            let stored = s.open_object(&object).await?;
            let meta = stored.meta;
            Ok(build_object_response(
                meta,
                headers.get(header::RANGE),
                ObjectPayload::MultiRing(s.clone(), stored),
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
        Backend::TokioUring(s) => {
            let stored = s.open_object(&object).await?;
            let meta = stored.meta;
            Ok(build_object_response(
                meta,
                headers.get(header::RANGE),
                ObjectPayload::TokioUring(s.clone(), stored),
            ))
        }
    }
}

async fn delete_object(State(backend): State<Backend>, uri: Uri) -> Result<Response, StoreError> {
    let object = parse_object(&uri)?;
    match &backend {
        Backend::MultiRing(s) => s.delete_object(&object).await?,
        Backend::Tokio(s) => s.delete_object(&object).await?,
        Backend::TokioUring(s) => s.delete_object(&object).await?,
    }
    Ok(empty_response(StatusCode::NO_CONTENT))
}

enum ObjectPayload {
    HeadOnly,
    MultiRing(Arc<MultiRingStore>, MultiRingStored),
    Tokio(Arc<TokioStore>, TokioStored),
    TokioUring(Arc<TokioUringStore>, TokioUringStored),
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
        ObjectPayload::MultiRing(store, stored) => {
            let head = builder.body(()).expect("valid response head");
            DirectResponse::new(head, store.writer(stored, range)).into_response()
        }
        ObjectPayload::Tokio(store, stored) => {
            let body = store.stream_body(stored, range);
            builder.body(body).expect("valid stream response")
        }
        ObjectPayload::TokioUring(store, stored) => {
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
