//! HTTP body utilities.

use crate::{BoxError, Error};
use bytes::Bytes;
use futures_core::{Stream, TryStream};
use http_body::{Body as _, Frame};
use pin_project_lite::pin_project;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use sync_wrapper::SyncWrapper;

type BoxBody = Pin<Box<dyn ErasedBody>>;

// Keep the original body's type available without exposing mutable access to
// its pinned storage. Error conversion happens in the wrapper below rather
// than in MapErr, so downcasting refers to the caller's body itself.
trait ErasedBody: http_body::Body<Data = Bytes, Error = Error> + Send {
    fn as_any(&self) -> &dyn Any;
}

impl fmt::Debug for dyn ErasedBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErasedBody").finish_non_exhaustive()
    }
}

pin_project! {
    struct ConcreteBody<B> {
        #[pin]
        body: B,
    }
}

impl<B> http_body::Body for ConcreteBody<B>
where
    B: http_body::Body<Data = Bytes>,
    B::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.project()
            .body
            .poll_frame(cx)
            .map(|frame| frame.map(|result| result.map_err(Error::new)))
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
}

impl<B> ErasedBody for ConcreteBody<B>
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    fn as_any(&self) -> &dyn Any {
        &self.body
    }
}

fn boxed<B>(body: B) -> BoxBody
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    Box::pin(ConcreteBody { body })
}

pub(crate) fn try_downcast<T, K>(k: K) -> Result<T, K>
where
    T: 'static,
    K: Send + 'static,
{
    let mut k = Some(k);
    if let Some(k) = <dyn std::any::Any>::downcast_mut::<Option<T>>(&mut k) {
        Ok(k.take().unwrap())
    } else {
        Err(k.unwrap())
    }
}

/// The body type used in axum requests and responses.
#[must_use]
#[derive(Debug)]
pub struct Body(BoxBody);

impl Body {
    /// Create a new `Body` that wraps another [`http_body::Body`].
    pub fn new<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        try_downcast(body).unwrap_or_else(|body| Self(boxed(body)))
    }

    /// Returns a reference to the body passed to [`Body::new`] if its type is `B`.
    ///
    /// This inspects only the directly wrapped body. It does not look through
    /// body combinators or middleware wrappers. Wrapping an existing `Body`
    /// with `Body::new` preserves the original body's identity.
    ///
    /// Only shared access is provided, so the underlying body remains pinned
    /// even when it does not implement [`Unpin`].
    #[must_use]
    pub fn downcast_ref<B: 'static>(&self) -> Option<&B> {
        self.0.as_ref().get_ref().as_any().downcast_ref()
    }

    /// Create an empty body.
    pub fn empty() -> Self {
        Self::new(http_body_util::Empty::new())
    }

    /// Create a new `Body` from a [`Stream`].
    ///
    /// [`Stream`]: https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: TryStream + Send + 'static,
        S::Ok: Into<Bytes>,
        S::Error: Into<BoxError>,
    {
        Self::new(StreamBody {
            stream: SyncWrapper::new(stream),
        })
    }

    /// Convert the body into a [`Stream`] of data frames.
    ///
    /// Non-data frames (such as trailers) will be discarded. Use [`http_body_util::BodyStream`] if
    /// you need a [`Stream`] of all frame types.
    ///
    /// [`http_body_util::BodyStream`]: https://docs.rs/http-body-util/latest/http_body_util/struct.BodyStream.html
    pub fn into_data_stream(self) -> BodyDataStream {
        BodyDataStream { inner: self }
    }
}

impl Default for Body {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<()> for Body {
    fn from(_: ()) -> Self {
        Self::empty()
    }
}

macro_rules! body_from_impl {
    ($ty:ty) => {
        impl From<$ty> for Body {
            fn from(buf: $ty) -> Self {
                Self::new(http_body_util::Full::from(buf))
            }
        }
    };
}

body_from_impl!(&'static [u8]);
body_from_impl!(std::borrow::Cow<'static, [u8]>);
body_from_impl!(Vec<u8>);

body_from_impl!(&'static str);
body_from_impl!(std::borrow::Cow<'static, str>);
body_from_impl!(String);

body_from_impl!(Bytes);

impl http_body::Body for Body {
    type Data = Bytes;
    type Error = Error;

    #[inline]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.0).poll_frame(cx)
    }

    #[inline]
    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }
}

/// A stream of data frames.
///
/// Created with [`Body::into_data_stream`].
#[must_use]
#[derive(Debug)]
pub struct BodyDataStream {
    inner: Body,
}

impl Stream for BodyDataStream {
    type Item = Result<Bytes, Error>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(Pin::new(&mut self.inner).poll_frame(cx)?) {
                Some(frame) => match frame.into_data() {
                    Ok(data) => return Poll::Ready(Some(Ok(data))),
                    Err(_frame) => {}
                },
                None => return Poll::Ready(None),
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size_hint = self.inner.size_hint();
        let lower = usize::try_from(size_hint.lower()).unwrap_or_default();
        let upper = size_hint.upper().and_then(|v| usize::try_from(v).ok());
        (lower, upper)
    }
}

impl http_body::Body for BodyDataStream {
    type Data = Bytes;
    type Error = Error;

    #[inline]
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    #[inline]
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

pin_project! {
    struct StreamBody<S> {
        #[pin]
        stream: SyncWrapper<S>,
    }
}

impl<S> http_body::Body for StreamBody<S>
where
    S: TryStream,
    S::Ok: Into<Bytes>,
    S::Error: Into<BoxError>,
{
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let stream = self.project().stream.get_pin_mut();
        match ready!(stream.try_poll_next(cx)) {
            Some(Ok(chunk)) => Poll::Ready(Some(Ok(Frame::data(chunk.into())))),
            Some(Err(err)) => Poll::Ready(Some(Err(Error::new(err)))),
            None => Poll::Ready(None),
        }
    }
}

#[test]
fn test_try_downcast() {
    assert_eq!(try_downcast::<i32, _>(5_u32), Err(5_u32));
    assert_eq!(try_downcast::<i32, _>(5_i32), Ok(5_i32));
}
