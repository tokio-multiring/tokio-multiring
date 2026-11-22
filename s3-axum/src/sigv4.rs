//! AWS Signature Version 4 (SigV4) request authentication and streaming
//! `aws-chunked` request body decoding (both signed and unsigned, with or
//! without trailing checksum headers).

use hmac::{Hmac, Mac};
use http::{HeaderMap, Method, Uri, header};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::ApiError;

type HmacSha256 = Hmac<Sha256>;

/// SHA-256 of the empty byte slice, lowercase hex.
pub(crate) const EMPTY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Static access/secret key pair for SigV4 verification.
#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub access_key: String,
    pub secret_key: String,
}

/// How the upload body should be decoded and/or hash-verified during `PUT`.
#[derive(Clone, Debug, Default)]
pub(crate) enum PayloadMode {
    /// Raw bytes, no payload hash or `aws-chunked` framing.
    #[default]
    Plain,
    /// Raw bytes whose SHA-256 digest must match the given 32 bytes.
    ExpectedSha256([u8; 32]),
    /// `aws-chunked` framing (signed per chunk or unsigned), optional trailers.
    AwsChunked(AwsChunkedSpec),
}

#[derive(Clone, Debug)]
pub(crate) struct AwsChunkedSpec {
    pub(crate) decoded_len: Option<u64>,
    pub(crate) signing: Option<ChunkSigning>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChunkSigning {
    signing_key: [u8; 32],
    amz_date: String,
    scope: String,
    prev_signature: String,
}

/// Verifies an incoming HTTP request against `auth` using AWS4-HMAC-SHA256 and
/// returns the [`PayloadMode`] the upload handler should enforce.
pub(crate) fn verify_request(
    auth: &AuthConfig,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<PayloadMode, ApiError> {
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::ACCESS_DENIED)?;

    let parsed = parse_authorization(auth_header).ok_or(ApiError::ACCESS_DENIED)?;
    if parsed
        .access_key
        .as_bytes()
        .ct_eq(auth.access_key.as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err(ApiError::INVALID_ACCESS_KEY);
    }

    let amz_date = headers
        .get("x-amz-date")
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::ACCESS_DENIED)?;

    let payload_header = headers
        .get("x-amz-content-sha256")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("UNSIGNED-PAYLOAD");

    let canonical_req =
        canonical_request(method, uri, headers, &parsed.signed_headers, payload_header);
    let scope = format!(
        "{}/{}/{}/aws4_request",
        parsed.date, parsed.region, parsed.service
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical_req.as_bytes())
    );

    let signing_key =
        derive_signing_key(&auth.secret_key, parsed.date, parsed.region, parsed.service);
    let expected_sig = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

    if expected_sig
        .as_bytes()
        .ct_eq(parsed.signature.as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err(ApiError::SIGNATURE_MISMATCH);
    }

    let decoded_len = headers
        .get("x-amz-decoded-content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());

    let is_aws_chunked_encoding = headers
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|tok| tok.trim().eq_ignore_ascii_case("aws-chunked"));

    match payload_header {
        "UNSIGNED-PAYLOAD" if !is_aws_chunked_encoding => Ok(PayloadMode::Plain),
        "UNSIGNED-PAYLOAD" | "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => {
            Ok(PayloadMode::AwsChunked(AwsChunkedSpec {
                decoded_len,
                signing: None,
            }))
        }
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => {
            Ok(PayloadMode::AwsChunked(AwsChunkedSpec {
                decoded_len,
                signing: Some(ChunkSigning {
                    signing_key,
                    amz_date: amz_date.to_owned(),
                    scope,
                    prev_signature: expected_sig,
                }),
            }))
        }
        hex_digest => {
            let bytes = hex::decode(hex_digest).map_err(|_| ApiError::BAD_REQUEST)?;
            let digest: [u8; 32] = bytes.try_into().map_err(|_| ApiError::BAD_REQUEST)?;
            if is_aws_chunked_encoding {
                Ok(PayloadMode::AwsChunked(AwsChunkedSpec {
                    decoded_len,
                    signing: None,
                }))
            } else {
                Ok(PayloadMode::ExpectedSha256(digest))
            }
        }
    }
}

/// Detects unsigned `aws-chunked` even when SigV4 auth is disabled (e.g. local
/// testing with SDKs configured without credentials).
pub(crate) fn detect_unauthenticated_mode(headers: &HeaderMap) -> PayloadMode {
    let is_aws_chunked = headers
        .get_all(header::CONTENT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|tok| tok.trim().eq_ignore_ascii_case("aws-chunked"))
        || headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("STREAMING-"));
    if is_aws_chunked {
        let decoded_len = headers
            .get("x-amz-decoded-content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        PayloadMode::AwsChunked(AwsChunkedSpec {
            decoded_len,
            signing: None,
        })
    } else {
        PayloadMode::Plain
    }
}

struct ParsedAuth<'a> {
    access_key: &'a str,
    date: &'a str,
    region: &'a str,
    service: &'a str,
    signed_headers: Vec<&'a str>,
    signature: &'a str,
}

fn parse_authorization(header: &str) -> Option<ParsedAuth<'_>> {
    let rest = header.strip_prefix("AWS4-HMAC-SHA256 ")?.trim();
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in rest.split(',') {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("Credential=") {
            credential = Some(val);
        } else if let Some(val) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(val);
        } else if let Some(val) = part.strip_prefix("Signature=") {
            signature = Some(val);
        }
    }

    let mut cred_parts = credential?.split('/');
    let access_key = cred_parts.next()?;
    let date = cred_parts.next()?;
    let region = cred_parts.next()?;
    let service = cred_parts.next()?;
    if cred_parts.next()? != "aws4_request" || cred_parts.next().is_some() {
        return None;
    }

    let signed_headers: Vec<&str> = signed_headers?
        .split(';')
        .filter(|h| !h.is_empty())
        .collect();
    if signed_headers.is_empty() {
        return None;
    }
    let signature = signature?;
    if signature.len() != 64 {
        return None;
    }
    Some(ParsedAuth {
        access_key,
        date,
        region,
        service,
        signed_headers,
        signature,
    })
}

fn canonical_request(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    signed_headers: &[&str],
    payload_hash: &str,
) -> String {
    let canonical_uri = if uri.path().is_empty() {
        "/"
    } else {
        uri.path()
    };
    let canonical_query = canonical_query_string(uri.query().unwrap_or(""));

    let mut canonical_headers = String::new();
    for &name in signed_headers {
        let mut values: Vec<String> = headers
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .map(collapse_whitespace)
            .collect();
        if values.is_empty() && name == "host" {
            // Shouldn't happen on HTTP/1.1, leave empty if absent.
            values.push(String::new());
        }
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(&values.join(","));
        canonical_headers.push('\n');
    }

    let signed_headers_str = signed_headers.join(";");
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers_str,
        payload_hash
    )
}

fn collapse_whitespace(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_space = false;
    for ch in input.trim().chars() {
        if ch.is_ascii_whitespace() {
            if !in_space {
                out.push(' ');
                in_space = true;
            }
        } else {
            out.push(ch);
            in_space = false;
        }
    }
    out
}

fn canonical_query_string(query: &str) -> String {
    if query.is_empty() {
        return String::new();
    }
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                aws_uri_encode(&percent_decode_lossy(k)),
                aws_uri_encode(&percent_decode_lossy(v)),
            )
        })
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_decode_lossy(input: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut bytes = input.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let h = bytes.next().and_then(|c| (c as char).to_digit(16));
            let l = bytes.next().and_then(|c| (c as char).to_digit(16));
            if let (Some(h), Some(l)) = (h, l) {
                out.push((h * 16 + l) as u8);
            }
        } else if b == b'+' {
            out.push(b' ');
        } else {
            out.push(b);
        }
    }
    out
}

fn aws_uri_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub(crate) fn derive_signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> [u8; 32] {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac_sha256(k_secret.as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

pub(crate) fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Streaming decoder for AWS S3 `aws-chunked` framing:
/// `<hex-len>[;chunk-signature=<64-hex>]\r\n<payload>\r\n...0[;chunk-signature=<64-hex>]\r\n[trailers]\r\n`
pub(crate) struct AwsChunkedDecoder {
    state: DecodeState,
    signing: Option<ChunkSigning>,
    line_buf: Vec<u8>,
    chunk_hasher: Sha256,
    expected_chunk_sig: Option<String>,
    decoded_total: u64,
    expected_decoded_len: Option<u64>,
}

enum DecodeState {
    HeaderLine,
    Data(usize),
    DataCr,
    DataLf,
    Trailers,
    Done,
}

impl AwsChunkedDecoder {
    pub(crate) fn new(spec: AwsChunkedSpec) -> Self {
        Self {
            state: DecodeState::HeaderLine,
            signing: spec.signing,
            line_buf: Vec::with_capacity(128),
            chunk_hasher: Sha256::new(),
            expected_chunk_sig: None,
            decoded_total: 0,
            expected_decoded_len: spec.decoded_len,
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        matches!(self.state, DecodeState::Done)
    }

    /// Consumes encoded bytes from `input` and writes decoded payload bytes
    /// into `output`. Returns `(input_consumed, output_written)`.
    pub(crate) fn decode(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(usize, usize), ApiError> {
        let mut in_pos = 0;
        let mut out_pos = 0;

        while in_pos < input.len() {
            match self.state {
                DecodeState::HeaderLine => {
                    let b = input[in_pos];
                    in_pos += 1;
                    self.line_buf.push(b);
                    if self.line_buf.len() > 256 {
                        return Err(ApiError::BAD_REQUEST);
                    }
                    if self.line_buf.ends_with(b"\r\n") {
                        let line = std::str::from_utf8(&self.line_buf[..self.line_buf.len() - 2])
                            .map_err(|_| ApiError::BAD_REQUEST)?;
                        let (size_hex, ext) = line.split_once(';').unwrap_or((line, ""));
                        let size = usize::from_str_radix(size_hex.trim(), 16)
                            .map_err(|_| ApiError::BAD_REQUEST)?;
                        let sig = ext
                            .split(';')
                            .find_map(|p| p.trim().strip_prefix("chunk-signature="))
                            .map(str::to_owned);
                        if self.signing.is_some() && sig.is_none() {
                            return Err(ApiError::SIGNATURE_MISMATCH);
                        }
                        self.expected_chunk_sig = sig;
                        self.chunk_hasher = Sha256::new();
                        self.line_buf.clear();

                        if size == 0 {
                            self.verify_chunk_signature()?;
                            self.state = DecodeState::Trailers;
                        } else {
                            self.state = DecodeState::Data(size);
                        }
                    }
                }
                DecodeState::Data(remaining) => {
                    if out_pos == output.len() {
                        break;
                    }
                    let take = remaining
                        .min(input.len() - in_pos)
                        .min(output.len() - out_pos);
                    let slice = &input[in_pos..in_pos + take];
                    output[out_pos..out_pos + take].copy_from_slice(slice);
                    if self.signing.is_some() {
                        self.chunk_hasher.update(slice);
                    }
                    in_pos += take;
                    out_pos += take;
                    self.decoded_total += take as u64;
                    let left = remaining - take;
                    if left == 0 {
                        self.verify_chunk_signature()?;
                        self.state = DecodeState::DataCr;
                    } else {
                        self.state = DecodeState::Data(left);
                    }
                }
                DecodeState::DataCr => {
                    if input[in_pos] != b'\r' {
                        return Err(ApiError::BAD_REQUEST);
                    }
                    in_pos += 1;
                    self.state = DecodeState::DataLf;
                }
                DecodeState::DataLf => {
                    if input[in_pos] != b'\n' {
                        return Err(ApiError::BAD_REQUEST);
                    }
                    in_pos += 1;
                    self.state = DecodeState::HeaderLine;
                }
                DecodeState::Trailers => {
                    let b = input[in_pos];
                    in_pos += 1;
                    self.line_buf.push(b);
                    if self.line_buf.len() > 4096 {
                        return Err(ApiError::BAD_REQUEST);
                    }
                    if self.line_buf.ends_with(b"\r\n") {
                        let is_empty_line = self.line_buf.len() == 2;
                        self.line_buf.clear();
                        if is_empty_line {
                            self.state = DecodeState::Done;
                            break;
                        }
                    }
                }
                DecodeState::Done => break,
            }
        }
        Ok((in_pos, out_pos))
    }

    pub(crate) fn finish(self) -> Result<(), ApiError> {
        if !self.is_done() {
            return Err(ApiError::BAD_REQUEST);
        }
        if let Some(expected) = self.expected_decoded_len
            && self.decoded_total != expected
        {
            return Err(ApiError::BAD_REQUEST);
        }
        Ok(())
    }

    fn verify_chunk_signature(&mut self) -> Result<(), ApiError> {
        let Some(ref mut signing) = self.signing else {
            return Ok(());
        };
        let provided = self
            .expected_chunk_sig
            .take()
            .ok_or(ApiError::SIGNATURE_MISMATCH)?;
        let chunk_hash = hex::encode(std::mem::take(&mut self.chunk_hasher).finalize());
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            signing.amz_date, signing.scope, signing.prev_signature, EMPTY_SHA256, chunk_hash
        );
        let expected = hmac_sha256_hex(&signing.signing_key, string_to_sign.as_bytes());
        if expected.as_bytes().ct_eq(provided.as_bytes()).unwrap_u8() != 1 {
            return Err(ApiError::SIGNATURE_MISMATCH);
        }
        signing.prev_signature = expected;
        Ok(())
    }
}

/// Helper used by tests (and clients) to compute a valid AWS4-HMAC-SHA256
/// `Authorization` header and optional signed `aws-chunked` body.
#[cfg(test)]
pub(crate) fn sign_request(
    auth: &AuthConfig,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    amz_date: &str,
    region: &str,
    payload_hash: &str,
) -> (String, String, [u8; 32]) {
    let date = &amz_date[..8];
    let mut names: Vec<&str> = headers.keys().map(|k| k.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    let canon = canonical_request(method, uri, headers, &names, payload_hash);
    let scope = format!("{date}/{region}/s3/aws4_request");
    let sts = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canon.as_bytes())
    );
    let key = derive_signing_key(&auth.secret_key, date, region, "s3");
    let sig = hmac_sha256_hex(&key, sts.as_bytes());
    let auth_value = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={}, Signature={sig}",
        auth.access_key,
        names.join(";")
    );
    (auth_value, sig, key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn official_aws_s3_get_object_vector() {
        // From AWS S3 SigV4 documentation: GET /test.txt example
        let auth = AuthConfig {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            "host",
            HeaderValue::from_static("examplebucket.s3.amazonaws.com"),
        );
        headers.insert("range", HeaderValue::from_static("bytes=0-9"));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static(EMPTY_SHA256),
        );
        headers.insert("x-amz-date", HeaderValue::from_static("20130524T000000Z"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
                 Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
            ),
        );
        let uri: Uri = "/test.txt".parse().unwrap();
        let mode = verify_request(&auth, &Method::GET, &uri, &headers).unwrap();
        assert!(matches!(mode, PayloadMode::ExpectedSha256(_)));
    }

    #[test]
    fn signed_aws_chunked_round_trip() {
        let auth = AuthConfig {
            access_key: "testkey".into(),
            secret_key: "testsecret".into(),
        };
        let amz_date = "20261122T040000Z";
        let region = "us-east-1";
        let uri: Uri = "/bucket/obj".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("host", HeaderValue::from_static("localhost:9000"));
        headers.insert("x-amz-date", HeaderValue::from_static(amz_date));
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"),
        );
        headers.insert(
            "x-amz-decoded-content-length",
            HeaderValue::from_static("11"),
        );
        let (auth_val, mut prev_sig, key) = sign_request(
            &auth,
            &Method::PUT,
            &uri,
            &headers,
            amz_date,
            region,
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
        );
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&auth_val).unwrap(),
        );

        let PayloadMode::AwsChunked(spec) =
            verify_request(&auth, &Method::PUT, &uri, &headers).unwrap()
        else {
            panic!("expected AwsChunked");
        };

        let scope = "20261122/us-east-1/s3/aws4_request";
        let mut wire = Vec::new();
        for chunk in [b"hello".as_slice(), b" world".as_slice(), b"".as_slice()] {
            let sts = format!(
                "AWS4-HMAC-SHA256-PAYLOAD\n{amz_date}\n{scope}\n{prev_sig}\n{EMPTY_SHA256}\n{}",
                sha256_hex(chunk)
            );
            prev_sig = hmac_sha256_hex(&key, sts.as_bytes());
            wire.extend_from_slice(
                format!("{:x};chunk-signature={prev_sig}\r\n", chunk.len()).as_bytes(),
            );
            wire.extend_from_slice(chunk);
            if !chunk.is_empty() {
                wire.extend_from_slice(b"\r\n");
            }
        }
        wire.extend_from_slice(b"x-amz-checksum-crc32:AAAAAA==\r\n\r\n");

        let mut dec = AwsChunkedDecoder::new(spec);
        let mut out = Vec::new();
        let mut pos = 0;
        let mut tmp = [0u8; 4]; // tiny buffer to test incremental decoding
        while pos < wire.len() {
            let (used, produced) = dec.decode(&wire[pos..], &mut tmp).unwrap();
            pos += used;
            out.extend_from_slice(&tmp[..produced]);
        }
        dec.finish().unwrap();
        assert_eq!(out, b"hello world");
    }
}
