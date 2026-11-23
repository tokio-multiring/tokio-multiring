//! Bounded write-behind for the standard Tokio storage backends.

use std::collections::VecDeque;
use std::io;
use tokio::task::JoinHandle;

pub(crate) struct WriteBehind<B> {
    window: usize,
    pending: VecDeque<JoinHandle<io::Result<Vec<B>>>>,
}

impl<B> WriteBehind<B> {
    pub(crate) fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            pending: VecDeque::new(),
        }
    }

    pub(crate) fn push(&mut self, round: JoinHandle<io::Result<Vec<B>>>) {
        assert!(self.pending.len() < self.window);
        self.pending.push_back(round);
    }

    /// At the window limit, reuse the oldest completed round's buffers.
    /// Otherwise allocate the next round so receiving overlaps queued writes.
    pub(crate) async fn next(&mut self, allocate: impl FnOnce() -> Vec<B>) -> io::Result<Vec<B>> {
        if self.pending.len() == self.window {
            self.pending
                .pop_front()
                .expect("window is nonzero")
                .await
                .map_err(io::Error::other)?
        } else {
            Ok(allocate())
        }
    }

    /// Always wait for every queued write, even after the first error. Files
    /// may only be synced, published, or removed once this has returned.
    pub(crate) async fn finish(&mut self) -> io::Result<()> {
        let mut first = None;
        while let Some(round) = self.pending.pop_front() {
            if let Err(error) = round
                .await
                .map_err(io::Error::other)
                .and_then(|result| result)
            {
                first.get_or_insert(error);
            }
        }
        first.map_or(Ok(()), Err)
    }
}

// Dropping an upload detaches these owned tasks: their file handles and
// buffers live until writing ends. Like the other stores, cancellation leaves
// temporary files for startup cleanup; it never unlinks a live writer's file.

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use std::task::Poll;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn window_waits_for_oldest_buffers_before_receiving_more() {
        let (tx, rx) = oneshot::channel();
        let mut writes = WriteBehind::new(2);
        writes.push(tokio::spawn(async move {
            rx.await.unwrap();
            Ok(vec![1u8])
        }));
        assert_eq!(writes.next(|| vec![2]).await.unwrap(), vec![2]);
        writes.push(tokio::spawn(async { Ok(vec![2]) }));
        let mut next = Box::pin(writes.next(|| panic!("must reuse at the limit")));
        poll_fn(|cx| {
            assert!(next.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        tx.send(()).unwrap();
        assert_eq!(next.await.unwrap(), vec![1]);
        writes.finish().await.unwrap();
    }

    #[tokio::test]
    async fn failure_drains_later_writes_before_cleanup_can_run() {
        let (tx, rx) = oneshot::channel();
        let mut writes = WriteBehind::<u8>::new(2);
        writes.push(tokio::spawn(async { Err(io::ErrorKind::WriteZero.into()) }));
        writes.push(tokio::spawn(async move {
            rx.await.unwrap();
            Ok(vec![2])
        }));
        let mut finish = Box::pin(writes.finish());
        tokio::task::yield_now().await;
        poll_fn(|cx| {
            assert!(finish.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        tx.send(()).unwrap();
        assert_eq!(finish.await.unwrap_err().kind(), io::ErrorKind::WriteZero);
    }

    #[tokio::test]
    async fn cancellation_keeps_owned_writes_alive() {
        let (tx, rx) = oneshot::channel();
        let (done, finished) = oneshot::channel();
        let mut writes = WriteBehind::<u8>::new(1);
        writes.push(tokio::spawn(async move {
            rx.await.unwrap();
            done.send(()).unwrap();
            Ok(vec![1])
        }));
        drop(writes);
        tx.send(()).unwrap();
        finished.await.unwrap();
    }
}
