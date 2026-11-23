//! Concrete body identity and pinning through the public Body API.

use axum_core::body::Body;
use bytes::Bytes;
use http_body::{Body as _, Frame};
use http_body_util::BodyExt;
use pin_project_lite::pin_project;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::task::{Wake, Waker};

pin_project! {
    struct NativeBody {
        identity: Arc<()>,
        address: AtomicUsize,
        polls: usize,
        #[pin]
        _pinned: PhantomPinned,
    }
}

impl NativeBody {
    fn new() -> Self {
        Self {
            identity: Arc::new(()),
            address: AtomicUsize::new(0),
            polls: 0,
            _pinned: PhantomPinned,
        }
    }
}

impl http_body::Body for NativeBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let address = self.as_ref().get_ref() as *const Self as usize;
        let previous = self.address.swap(address, Ordering::Relaxed);
        assert!(previous == 0 || previous == address, "pinned body moved");
        let this = self.project();
        *this.polls += 1;
        match *this.polls {
            1 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            2 => Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"payload"))))),
            3 => Poll::Ready(Some(Err(std::io::Error::other("native error")))),
            _ => Poll::Ready(None),
        }
    }
}

#[test]
fn downcast_ref_preserves_concrete_identity() {
    let native = NativeBody::new();
    let identity = native.identity.clone();
    let body = Body::new(native);
    let concrete = body.downcast_ref::<NativeBody>().unwrap();
    assert!(Arc::ptr_eq(&concrete.identity, &identity));
    assert!(body
        .downcast_ref::<http_body_util::Empty<Bytes>>()
        .is_none());
}

#[test]
fn downcast_ref_does_not_look_through_wrappers() {
    let body = Body::new(NativeBody::new());
    let wrapped = Body::new(body.map_frame(|frame| frame));
    assert!(wrapped.downcast_ref::<NativeBody>().is_none());
}

#[test]
fn wrapping_an_existing_body_preserves_identity() {
    let body = Body::new(NativeBody::new());
    let address = body.downcast_ref::<NativeBody>().unwrap() as *const NativeBody;
    let rewrapped = Body::new(body);
    assert_eq!(
        rewrapped.downcast_ref::<NativeBody>().unwrap() as *const _,
        address
    );
}

#[test]
fn replacing_a_body_of_the_same_type_changes_identity() {
    let original = Body::new(NativeBody::new());
    let replacement = Body::new(NativeBody::new());
    let identity = original
        .downcast_ref::<NativeBody>()
        .unwrap()
        .identity
        .clone();
    let body = replacement;
    assert!(!Arc::ptr_eq(
        &body.downcast_ref::<NativeBody>().unwrap().identity,
        &identity,
    ));
}

#[test]
fn downcasting_and_moving_the_wrapper_preserve_pinned_storage_and_errors() {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    let mut body = Body::new(NativeBody::new());
    assert!(Pin::new(&mut body).poll_frame(&mut cx).is_pending());
    let address = body.downcast_ref::<NativeBody>().unwrap() as *const NativeBody;
    let mut moved = Box::new(body);
    assert_eq!(
        moved.downcast_ref::<NativeBody>().unwrap() as *const _,
        address
    );
    let Poll::Ready(Some(Ok(frame))) = Pin::new(&mut *moved).poll_frame(&mut cx) else {
        panic!("expected data frame");
    };
    assert_eq!(frame.into_data().unwrap(), "payload");
    let Poll::Ready(Some(Err(error))) = Pin::new(&mut *moved).poll_frame(&mut cx) else {
        panic!("expected mapped error");
    };
    assert_eq!(error.to_string(), "native error");
    assert!(matches!(
        Pin::new(&mut *moved).poll_frame(&mut cx),
        Poll::Ready(None)
    ));
}
