//! Bucket and key names, and where they live on disk.
//!
//! A bucket is a directory under each root, and an object is named by one
//! file in it. Keys are arbitrary bytes, so a key is encoded into a single
//! file name: anything but ASCII letters, digits, `-`, `_` and `.` is
//! written as `%XX`, and so is a leading `.`. That keeps every key inside
//! its bucket's directory and leaves names starting with `.` to the server.

use std::ffi::CString;

use crate::error::ApiError;

/// The longest file name the file systems this runs on allow.
const NAME_MAX: usize = 255;

/// Starts the names of files that are still being uploaded.
pub(crate) const UPLOAD_PREFIX: &str = ".upload-";

/// Starts the names of the files that hold the parts of an object other
/// than its first.
pub(crate) const PART_PREFIX: &str = ".part-";

/// What a request path names.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Target {
    Service,
    Bucket(Bucket),
    Object(Object),
}

/// A valid bucket name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Bucket(String);

/// An object's place on disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Object {
    pub(crate) bucket: Bucket,
    /// The key, encoded as a file name.
    file: String,
}

impl Target {
    /// Reads the path of a request, as it came off the wire.
    pub(crate) fn parse(path: &str) -> Result<Target, ApiError> {
        let path = path.strip_prefix('/').ok_or(ApiError::INVALID_URI)?;
        // Split before decoding: an encoded slash belongs to the key.
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
    /// S3's naming rules: 3 to 63 lower-case letters, digits, dots and
    /// hyphens, starting and ending with a letter or digit, without two dots
    /// in a row. A name that passes is also a safe directory name.
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

    /// The bucket's directory, relative to the root.
    pub(crate) fn path(&self) -> CString {
        c_string(self.0.clone())
    }

    /// A file in the bucket's directory, relative to the root.
    pub(crate) fn file(&self, name: &str) -> CString {
        c_string(format!("{}/{name}", self.0))
    }
}

impl Object {
    /// The file that names the object, relative to a root.
    pub(crate) fn path(&self) -> CString {
        self.bucket.file(&self.file)
    }

    /// A number that is the same for this object in every run of the
    /// server: the 64-bit FNV-1a hash of its path.
    pub(crate) fn hash(&self) -> u64 {
        let path = [self.bucket.0.as_bytes(), b"/", self.file.as_bytes()];
        path.into_iter()
            .flatten()
            .fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    }

    /// Which of `drives` drives holds the object's first part. Spreading
    /// first parts over the drives spreads the objects that have no other.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn object_path(path: &str) -> String {
        match Target::parse(path).unwrap() {
            Target::Object(object) => object.path().into_string().unwrap(),
            other => panic!("{path} is {other:?}"),
        }
    }

    #[test]
    fn paths_name_the_service_a_bucket_or_an_object() {
        assert_eq!(Target::parse("/").unwrap(), Target::Service);
        for path in ["/photos", "/photos/", "/%70hotos"] {
            let Target::Bucket(bucket) = Target::parse(path).unwrap() else {
                panic!("{path} is not a bucket");
            };
            assert_eq!(bucket.path().to_str().unwrap(), "photos");
        }
        assert_eq!(object_path("/photos/cat.jpg"), "photos/cat.jpg");
    }

    #[test]
    fn keys_stay_inside_their_bucket() {
        assert_eq!(object_path("/photos/2026/cat.jpg"), "photos/2026%2Fcat.jpg");
        assert_eq!(object_path("/photos/a%2Fb"), "photos/a%2Fb");
        assert_eq!(object_path("/photos/.."), "photos/%2E.");
        assert_eq!(
            object_path("/photos/../../etc/passwd"),
            "photos/%2E.%2F..%2Fetc%2Fpasswd"
        );
        assert_eq!(object_path("/photos/.upload-1"), "photos/%2Eupload-1");
        assert_eq!(object_path("/photos//"), "photos/%2F");
        assert_eq!(object_path("/photos/a%00b"), "photos/a%00b");
        assert_eq!(object_path("/photos/100%25"), "photos/100%25");
        assert_eq!(
            object_path("/photos/na%C3%AFve%20caf%c3%a9"),
            "photos/na%C3%AFve%20caf%C3%A9"
        );
    }

    /// Where an object lives must not change from one build to the next.
    #[test]
    fn placement_is_stable() {
        let Target::Object(object) = Target::parse("/photos/cat.jpg").unwrap() else {
            panic!("not an object");
        };
        assert_eq!(object.hash(), 0x4664_4eaa_9e47_eeb2);
        assert_eq!(object.first_drive(1), 0);
        assert_eq!(object.first_drive(2), 0);
        let Target::Object(object) = Target::parse("/photos/2026/cat.jpg").unwrap() else {
            panic!("not an object");
        };
        assert_eq!(object.first_drive(2), 1);
    }

    #[test]
    fn distinct_keys_get_distinct_files() {
        assert_ne!(object_path("/photos/a%2Fb"), object_path("/photos/a%252Fb"));
        assert_ne!(object_path("/photos/a/b"), object_path("/photos/a%2Fb/"));
    }

    #[test]
    fn bad_names_are_refused() {
        for path in [
            "/ab",
            "/Photos",
            "/pho_tos",
            "/-photos",
            "/photos-",
            "/pho..tos",
            "/.photos",
            "/..",
            "/../x",
            "/ph%2Fotos/x",
            "//key",
            "/%ffabc/x",
        ] {
            assert_eq!(
                Target::parse(path),
                Err(ApiError::INVALID_BUCKET_NAME),
                "{path}"
            );
        }
        for path in ["photos", "/photos/%", "/photos/%4", "/photos/%zz", ""] {
            assert_eq!(Target::parse(path), Err(ApiError::INVALID_URI), "{path}");
        }
        let fits = format!("/photos/{}", "k".repeat(255));
        assert!(Target::parse(&fits).is_ok());
        let long = format!("/photos/{}", "k".repeat(256));
        assert_eq!(Target::parse(&long), Err(ApiError::KEY_TOO_LONG));
        let long = format!("/photos/{}", "%20".repeat(86));
        assert_eq!(Target::parse(&long), Err(ApiError::KEY_TOO_LONG));
    }
}
