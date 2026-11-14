//! The errors the API reports, in S3's vocabulary.

use http::StatusCode;

/// An error response: a status, and S3's name for what went wrong.
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
    /// The response body. Codes and messages are plain text that needs no
    /// escaping.
    pub(crate) fn body(&self) -> Vec<u8> {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>{}</Code><Message>{}</Message></Error>",
            self.code, self.message
        )
        .into_bytes()
    }
}
