//! Bounded, ordered read-ahead and recyclable owned response buffers.

use std::collections::VecDeque;
use std::io;
use std::ops::{Deref, DerefMut, Range};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use tokio::task::JoinHandle;

type Round = JoinHandle<io::Result<Vec<Bytes>>>;

/// Starts at most `window` rounds, emitting their slices in object order.
/// Dropping the response aborts queued async work. A blocking read already
/// executing still owns its buffer and finishes safely in the blocking pool.
pub(crate) struct ReadAhead {
    rounds: Range<u64>,
    window: usize,
    start: Box<dyn FnMut(u64) -> Round + Send>,
    pending: VecDeque<Round>,
    ready: VecDeque<Bytes>,
}

impl ReadAhead {
    pub(crate) fn new(
        rounds: Range<u64>,
        window: usize,
        start: impl FnMut(u64) -> Round + Send + 'static,
    ) -> Self {
        assert!(window > 0);
        Self {
            rounds,
            window,
            start: Box::new(start),
            pending: VecDeque::new(),
            ready: VecDeque::new(),
        }
    }

    fn stop(&mut self) {
        self.rounds = 0..0;
        for round in self.pending.drain(..) {
            round.abort();
        }
    }
}

impl Stream for ReadAhead {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Do not refill while emitting a completed round: it still counts
        // towards the window and can contain several stripe slices.
        if let Some(bytes) = this.ready.pop_front() {
            return Poll::Ready(Some(Ok(bytes)));
        }
        while this.pending.len() < this.window {
            let Some(round) = this.rounds.next() else {
                break;
            };
            this.pending.push_back((this.start)(round));
        }
        let Some(first) = this.pending.front_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(first).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.pending.pop_front();
                match result.map_err(io::Error::other).and_then(|result| result) {
                    Ok(bytes) => {
                        this.ready = bytes.into();
                        Poll::Ready(this.ready.pop_front().map(Ok))
                    }
                    Err(error) => {
                        this.stop();
                        Poll::Ready(Some(Err(error)))
                    }
                }
            }
        }
    }
}

impl Drop for ReadAhead {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The pool only caches released buffers. Bytes retained by the HTTP consumer
/// keep their own allocation; they can never be overwritten by a later read.
pub(crate) struct BufferCache<B> {
    free: Mutex<Vec<B>>,
    limit: usize,
}

impl<B> BufferCache<B> {
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            free: Mutex::new(Vec::new()),
            limit,
        })
    }

    pub(crate) fn take(self: &Arc<Self>, create: impl FnOnce() -> B) -> Recycled<B> {
        let buffer = self.free.lock().unwrap_or_else(|e| e.into_inner()).pop();
        Recycled {
            buffer: Some(buffer.unwrap_or_else(create)),
            cache: self.clone(),
        }
    }
}

pub(crate) struct Recycled<B> {
    buffer: Option<B>,
    cache: Arc<BufferCache<B>>,
}

impl<B> Deref for Recycled<B> {
    type Target = B;
    fn deref(&self) -> &B {
        self.buffer.as_ref().expect("buffer exists until drop")
    }
}

impl<B> DerefMut for Recycled<B> {
    fn deref_mut(&mut self) -> &mut B {
        self.buffer.as_mut().expect("buffer exists until drop")
    }
}

impl<B: AsRef<[u8]>> AsRef<[u8]> for Recycled<B> {
    fn as_ref(&self) -> &[u8] {
        self.deref().as_ref()
    }
}

impl<B> Drop for Recycled<B> {
    fn drop(&mut self) {
        let mut free = self.cache.free.lock().unwrap_or_else(|e| e.into_inner());
        if free.len() < self.cache.limit {
            free.push(self.buffer.take().expect("buffer exists until drop"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn read_ahead_is_bounded_ordered_and_stops_on_error() {
        let started = Arc::new(AtomicUsize::new(0));
        let mut senders = VecDeque::new();
        let mut receivers = VecDeque::new();
        for _ in 0..4 {
            let (tx, rx) = oneshot::channel();
            senders.push_back(tx);
            receivers.push_back(rx);
        }
        let count = started.clone();
        let mut stream = ReadAhead::new(0..4, 2, move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            let rx = receivers.pop_front().unwrap();
            tokio::spawn(async move { rx.await.unwrap() })
        });
        poll_fn(|cx| {
            assert!(Pin::new(&mut stream).poll_next(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(started.load(Ordering::SeqCst), 2);
        let first = senders.pop_front().unwrap();
        senders
            .pop_front()
            .unwrap()
            .send(Ok(vec![Bytes::from_static(b"second")]))
            .unwrap();
        tokio::task::yield_now().await;
        poll_fn(|cx| {
            assert!(Pin::new(&mut stream).poll_next(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        first
            .send(Ok(vec![
                Bytes::from_static(b"first-a"),
                Bytes::from_static(b"first-b"),
            ]))
            .unwrap();
        for expected in [b"first-a".as_slice(), b"first-b", b"second"] {
            let actual = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(actual, expected);
            if expected == b"first-b" {
                assert_eq!(
                    started.load(Ordering::SeqCst),
                    2,
                    "emitting slices must not expand the window"
                );
            }
        }
        senders
            .pop_front()
            .unwrap()
            .send(Err(io::ErrorKind::UnexpectedEof.into()))
            .unwrap();
        assert!(
            poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
                .await
                .unwrap()
                .is_err()
        );
        assert!(
            poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
                .await
                .is_none()
        );
        tokio::task::yield_now().await;
        assert!(
            senders.pop_front().unwrap().is_closed(),
            "error must abort later reads"
        );
    }

    #[tokio::test]
    async fn dropping_response_aborts_read_ahead() {
        let (tx, rx) = oneshot::channel::<()>();
        let mut rx = Some(rx);
        let mut stream = ReadAhead::new(0..1, 1, move |_| {
            let rx = rx.take().unwrap();
            tokio::spawn(async move {
                let _ = rx.await;
                Ok(vec![Bytes::new()])
            })
        });
        poll_fn(|cx| {
            assert!(Pin::new(&mut stream).poll_next(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(stream);
        tokio::task::yield_now().await;
        assert!(tx.is_closed());
    }

    #[test]
    fn slices_hold_buffer_until_the_last_owner_releases_it() {
        let cache = BufferCache::new(1);
        let bytes = Bytes::from_owner(cache.take(|| vec![1, 2, 3]));
        let slice = bytes.slice(1..);
        drop(bytes);
        assert!(cache.free.lock().unwrap().is_empty());
        assert_eq!(slice.as_ref(), &[2, 3]);
        drop(slice);
        assert_eq!(
            &**cache.take(|| panic!("buffer should be recycled")),
            &[1, 2, 3]
        );
    }
}
