//! Bucket and key names, and where they live on disk.

use std::ffi::CString;

use crate::error::ApiError;

const NAME_MAX: usize = 255;

pub(crate) const UPLOAD_PREFIX: &str = ".upload-";
pub(crate) const PART_PREFIX: &str = ".part-";

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Service,
    Bucket(Bucket),
    Object(Object),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Bucket(String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Object {
    pub(crate) bucket: Bucket,
    file: String,
}

impl Target {
    pub(crate) fn parse(path: &str) -> Result<Target, ApiError> {
        let path = path.strip_prefix('/').ok_or(ApiError::INVALID_URI)?;
        let (bucket, key) = path.split_once('/').unwrap_or((path, ""));
        if bucket.is_empty() {
            return if key.is_empty() {
                Ok(Target::Service)
            } else {
                Err(ApiError::INVALID_BUCKET_NAME)
            };
        }

        let bucket = percent_decode(bucket)?;
        let bucket = Bucket::new(bucket).ok_or(ApiError::INVALID_BUCKET_NAME)?;
        if key.is_empty() {
            return Ok(Target::Bucket(bucket));
        }

        let file = encode_key(&percent_decode(key)?);
        if file.len() > NAME_MAX {
            return Err(ApiError::KEY_TOO_LONG);
        }
        Ok(Target::Object(Object { bucket, file }))
    }
}

impl Bucket {
    fn new(name: Vec<u8>) -> Option<Bucket> {
        let name = String::from_utf8(name).ok()?;
        let bytes = name.as_bytes();
        let edge = |byte: &u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        let valid = (3..=63).contains(&bytes.len())
            && bytes
                .iter()
                .all(|byte| edge(byte) || matches!(byte, b'-' | b'.'))
            && bytes.first().is_some_and(edge)
            && bytes.last().is_some_and(edge)
            && !name.contains("..");
        valid.then_some(Bucket(name))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn path(&self) -> CString {
        c_string(self.0.clone())
    }

    pub(crate) fn file(&self, name: &str) -> CString {
        c_string(format!("{}/{name}", self.0))
    }

    pub(crate) fn file_rel(&self, name: &str) -> String {
        format!("{}/{name}", self.0)
    }
}

impl Object {
    pub(crate) fn path(&self) -> CString {
        self.bucket.file(&self.file)
    }

    pub(crate) fn path_rel(&self) -> String {
        self.bucket.file_rel(&self.file)
    }

    pub(crate) fn hash(&self) -> u64 {
        let path = [self.bucket.0.as_bytes(), b"/", self.file.as_bytes()];
        path.into_iter()
            .flatten()
            .fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    }

    pub(crate) fn first_drive(&self, drives: usize) -> usize {
        (self.hash() % drives as u64) as usize
    }
}

fn c_string(path: String) -> CString {
    CString::new(path).expect("bucket and file names are free of NUL bytes")
}

fn percent_decode(text: &str) -> Result<Vec<u8>, ApiError> {
    let mut decoded = Vec::with_capacity(text.len());
    let mut bytes = text.bytes();
    while let Some(byte) = bytes.next() {
        if byte != b'%' {
            decoded.push(byte);
            continue;
        }
        let digit = |byte: Option<u8>| {
            byte.and_then(|byte| (byte as char).to_digit(16))
                .ok_or(ApiError::INVALID_URI)
        };
        let high = digit(bytes.next())?;
        let low = digit(bytes.next())?;
        decoded.push((high * 16 + low) as u8);
    }
    Ok(decoded)
}

fn encode_key(key: &[u8]) -> String {
    let mut file = String::with_capacity(key.len());
    for (index, &byte) in key.iter().enumerate() {
        let plain = byte.is_ascii_alphanumeric()
            || matches!(byte, b'-' | b'_')
            || (byte == b'.' && index > 0);
        if plain {
            file.push(byte as char);
        } else {
            file.push_str(&format!("%{byte:02X}"));
        }
    }
    file
}
