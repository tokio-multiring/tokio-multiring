//! S3 API errors as an Axum [`IntoResponse`] type.

use std::io;

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use http::{HeaderValue, StatusCode, header};

use crate::date;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    code: &'static str,
    message: &'static str,
}

macro_rules! errors {
    ($($name:ident = $status:ident, $code:literal, $message:literal;)*) => {
        impl ApiError {
            $(pub(crate) const $name: ApiError = ApiError {
                status: StatusCode::$status,
                code: $code,
                message: $message,
            };)*
        }
    };
}

errors! {
    BAD_REQUEST = BAD_REQUEST, "BadRequest", "The request body is malformed.";
    BUCKET_EXISTS = CONFLICT, "BucketAlreadyOwnedByYou", "The bucket already exists.";
    BUCKET_NOT_EMPTY = CONFLICT, "BucketNotEmpty", "The bucket is not empty.";
    INTERNAL = INTERNAL_SERVER_ERROR, "InternalError", "The server could not complete the request.";
    INVALID_BUCKET_NAME = BAD_REQUEST, "InvalidBucketName", "The bucket name is not valid.";
    INVALID_RANGE = RANGE_NOT_SATISFIABLE, "InvalidRange", "The requested range is not satisfiable.";
    INVALID_URI = BAD_REQUEST, "InvalidURI", "The request path could not be parsed.";
    KEY_TOO_LONG = BAD_REQUEST, "KeyTooLongError", "The key is too long.";
    METHOD_NOT_ALLOWED = METHOD_NOT_ALLOWED, "MethodNotAllowed", "The method is not allowed on this resource.";
    NOT_IMPLEMENTED = NOT_IMPLEMENTED, "NotImplemented", "The operation is not implemented.";
    NO_SUCH_BUCKET = NOT_FOUND, "NoSuchBucket", "The bucket does not exist.";
    NO_SUCH_KEY = NOT_FOUND, "NoSuchKey", "The key does not exist.";
}

impl ApiError {
    pub(crate) fn body(&self) -> Vec<u8> {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>{}</Code><Message>{}</Message></Error>",
            self.code, self.message
        )
        .into_bytes()
    }

    pub(crate) fn into_response_with_header(
        self,
        name: header::HeaderName,
        value: HeaderValue,
    ) -> Response {
        let mut resp = self.into_response();
        resp.headers_mut().insert(name, value);
        resp
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = self.body();
        Response::builder()
            .status(self.status)
            .header(header::DATE, date::now())
            .header(header::CONTENT_TYPE, "application/xml")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .expect("valid error response")
    }
}

pub(crate) enum StoreError {
    Api(ApiError),
    Io(io::Error),
}

impl From<io::Error> for StoreError {
    fn from(error: io::Error) -> Self {
        StoreError::Io(error)
    }
}

impl From<ApiError> for StoreError {
    fn from(error: ApiError) -> Self {
        StoreError::Api(error)
    }
}

impl IntoResponse for StoreError {
    fn into_response(self) -> Response {
        match self {
            StoreError::Api(err) => err.into_response(),
            StoreError::Io(err) => {
                log::error!("storage failed: {err}");
                ApiError::INTERNAL.into_response()
            }
        }
    }
}
