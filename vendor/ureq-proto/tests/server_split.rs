#![cfg(feature = "server")]

use http::{Response, StatusCode};
use ureq_proto::Error;
use ureq_proto::server::state::{Cleanup, ProvideResponse};
use ureq_proto::server::{Reply, RequestBody, SendResponseResult};

fn split(head: &[u8]) -> (RequestBody, Reply<ProvideResponse>) {
    let mut request = Reply::new().unwrap();
    let (used, parsed) = request.try_request(head).unwrap();
    assert!(parsed.is_some());
    assert_eq!(used, head.len());
    request.split().unwrap()
}

fn respond(
    reply: Reply<ProvideResponse>,
    response: Response<()>,
    payload: &[u8],
) -> (Vec<u8>, Reply<Cleanup>) {
    let mut writer = reply.provide(response).unwrap();
    let mut output = Vec::new();
    let mut scratch = [0; 128];
    while !writer.is_finished() {
        let used = writer.write(&mut scratch).unwrap();
        assert_ne!(used, 0);
        output.extend_from_slice(&scratch[..used]);
    }
    let cleanup = match writer.proceed() {
        SendResponseResult::Cleanup(cleanup) => cleanup,
        SendResponseResult::SendBody(mut body) => {
            let mut rest = payload;
            while !rest.is_empty() {
                let (used, produced) = body.write(rest, &mut scratch).unwrap();
                assert!(used != 0 || produced != 0);
                output.extend_from_slice(&scratch[..produced]);
                rest = &rest[used..];
            }
            while !body.is_finished() {
                let (_, produced) = body.write(&[], &mut scratch).unwrap();
                output.extend_from_slice(&scratch[..produced]);
                assert!(produced != 0 || body.is_finished());
            }
            body.proceed()
        }
    };
    (output, cleanup)
}

fn response(size: usize) -> Response<()> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-length", size)
        .body(())
        .unwrap()
}

fn decode_fragments(body: &mut RequestBody, fragments: &[&[u8]]) -> (Vec<u8>, Vec<u8>) {
    let mut decoded = Vec::new();
    let mut pending = Vec::new();
    let mut scratch = [0; 3];
    for fragment in fragments {
        pending.extend_from_slice(fragment);
        while !body.is_ended() && !pending.is_empty() {
            let (used, produced) = body.read(&pending, &mut scratch).unwrap();
            decoded.extend_from_slice(&scratch[..produced]);
            pending.drain(..used);
            if used == 0 && produced == 0 {
                break;
            }
        }
    }
    (decoded, pending)
}

#[test]
fn incomplete_request_head_cannot_split() {
    let mut request = Reply::new().unwrap();
    assert!(
        request
            .try_request(b"POST / HTTP/1.1\r\nHost: x\r\n")
            .unwrap()
            .1
            .is_none()
    );
    assert!(request.split().is_none());
}

#[test]
fn length_delimited_fragments_preserve_pipelined_request() {
    let (mut body, reply) = split(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 7\r\n\r\n");
    assert_eq!(body.remaining(), Some(7));
    let (decoded, pending) = decode_fragments(
        &mut body,
        &[b"a", b"bc", b"de", b"fgGET /next HTTP/1.1\r\n\r\n"],
    );
    assert_eq!(decoded, b"abcdefg");
    assert_eq!(pending, b"GET /next HTTP/1.1\r\n\r\n");
    assert!(body.is_ended());
    assert_eq!(body.remaining(), Some(0));
    assert_eq!(body.read(&pending, &mut [0; 32]).unwrap(), (0, 0));
    let (_, cleanup) = respond(reply, response(2), b"ok");
    assert!(!cleanup.must_close_connection());
}

#[test]
fn chunked_fragments_preserve_pipelined_request() {
    let (mut body, reply) =
        split(b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n");
    assert_eq!(body.remaining(), None);
    let (decoded, pending) = decode_fragments(
        &mut body,
        &[
            b"3\r",
            b"\nabc\r",
            b"\n4\r\ndef",
            b"g\r\n0\r",
            b"\n\r\nGET /next HTTP/1.1\r\n\r\n",
        ],
    );
    assert_eq!(decoded, b"abcdefg");
    assert_eq!(pending, b"GET /next HTTP/1.1\r\n\r\n");
    assert!(body.is_ended());
    let (_, cleanup) = respond(reply, response(2), b"ok");
    assert!(!cleanup.must_close_connection());
}

#[test]
fn response_can_finish_before_request_body() {
    let (mut body, reply) = split(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 7\r\n\r\n");
    let (wire, cleanup) = respond(reply, response(2), b"ok");
    assert!(wire.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(wire.ends_with(b"\r\n\r\nok"));
    assert!(!cleanup.must_close_connection());
    // The transport must additionally check this before reusing the connection.
    assert!(!body.is_ended());
    assert_eq!(body.read(b"abcdefgNEXT", &mut [0; 16]).unwrap(), (7, 7));
    assert!(body.is_ended());
}

#[test]
fn head_response_preserves_representation_headers_without_payload() {
    let (body, reply) = split(b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(body.is_ended());
    let (wire, cleanup) = respond(reply, response(7), b"ignored");
    let wire = String::from_utf8(wire).unwrap();
    assert!(wire.to_ascii_lowercase().contains("content-length: 7\r\n"));
    assert!(wire.ends_with("\r\n\r\n"));
    assert!(!cleanup.must_close_connection());
}

#[test]
fn request_version_and_connection_close_survive_split() {
    for (head, must_close, keep_alive) in [
        (&b"GET / HTTP/1.0\r\n\r\n"[..], true, false),
        (
            &b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"[..],
            false,
            true,
        ),
        (
            &b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"[..],
            true,
            false,
        ),
    ] {
        let (body, reply) = split(head);
        assert!(body.is_ended());
        let (wire, cleanup) = respond(reply, response(2), b"ok");
        assert_eq!(cleanup.must_close_connection(), must_close);
        let wire = String::from_utf8(wire).unwrap().to_ascii_lowercase();
        assert_eq!(wire.contains("connection: keep-alive\r\n"), keep_alive);
        assert!(!wire.contains("transfer-encoding:"));
    }
}

#[test]
fn http10_without_response_length_uses_connection_close() {
    let (_, reply) = split(b"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
    let (wire, cleanup) = respond(reply, Response::new(()), b"payload");
    let wire = String::from_utf8(wire).unwrap().to_ascii_lowercase();
    assert!(!wire.contains("transfer-encoding:"));
    assert!(!wire.contains("connection: keep-alive"));
    assert!(wire.ends_with("\r\n\r\npayload"));
    assert!(cleanup.must_close_connection());
}

#[test]
fn continue_is_once_only_and_survives_output_overflow() {
    let (mut body, reply) =
        split(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nExpect: 100-continue\r\n\r\n");
    assert!(body.needs_continue());
    assert!(matches!(
        body.accept_continue(&mut [0; 5]),
        Err(Error::OutputOverflow)
    ));
    assert!(body.needs_continue());
    let mut output = [0; 128];
    let count = body.accept_continue(&mut output).unwrap();
    assert_eq!(&output[..count], b"HTTP/1.1 100 Continue\r\n\r\n");
    assert!(!body.needs_continue());
    assert_eq!(body.accept_continue(&mut []).unwrap(), 0);
    body.consume_direct_read(1).unwrap();
    let (_, cleanup) = respond(reply, response(2), b"ok");
    assert!(!cleanup.must_close_connection());
}

#[test]
fn zero_length_and_unframed_bodies_need_no_continue() {
    for head in [
        &b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\nExpect: 100-continue\r\n\r\n"[..],
        &b"POST / HTTP/1.1\r\nHost: x\r\nExpect: 100-continue\r\n\r\n"[..],
    ] {
        let (mut body, reply) = split(head);
        assert!(body.is_ended());
        assert!(!body.needs_continue());
        assert_eq!(body.accept_continue(&mut []).unwrap(), 0);
        assert_eq!(
            body.read(b"GET /next HTTP/1.1\r\n\r\n", &mut [0; 32])
                .unwrap(),
            (0, 0)
        );
        let (_, cleanup) = respond(reply, response(2), b"ok");
        assert!(!cleanup.must_close_connection());
    }
}

#[test]
fn direct_accounting_rejects_overflow_without_changing_remaining() {
    let (mut body, _) = split(b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\n");
    assert!(matches!(
        body.consume_direct_read(4),
        Err(Error::BodyLargerThanContentLength)
    ));
    assert_eq!(body.remaining(), Some(3));
    body.consume_direct_read(3).unwrap();
    assert!(body.is_ended());
    let (mut chunked, _) =
        split(b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n");
    assert!(matches!(
        chunked.consume_direct_read(1),
        Err(Error::BodyIsChunked)
    ));
    assert!(!chunked.is_ended());
}

#[test]
fn malformed_chunk_size_is_rejected() {
    let (mut body, _) = split(b"POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n");
    assert!(body.read(b"not-hex\r\nabc\r\n", &mut [0; 32]).is_err());
    assert!(!body.is_ended());
}

#[test]
fn split_response_encoder_rejects_informational_status() {
    let (_, reply) = split(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(matches!(
        reply.provide(Response::builder().status(100).body(()).unwrap()),
        Err(Error::BadReject100Status(_))
    ));
}
