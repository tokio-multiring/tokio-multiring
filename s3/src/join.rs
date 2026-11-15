use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

/// Runs the futures side by side, and returns what they produced in the
/// order they were given.
pub(crate) async fn join_all<F: Future>(mut futures: Vec<F>) -> Vec<F::Output> {
    if futures.len() == 1 {
        let only = futures.pop().expect("there is one");
        return vec![only.await];
    }

    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut outputs: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (future, output) in futures.iter_mut().zip(&mut outputs) {
            if output.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => *output = Some(value),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    outputs
        .into_iter()
        .map(|output| output.expect("every future has finished"))
        .collect()
}

/// A future that has been polled once, so that the operations it begins
/// with are under way, to be finished later.
///
/// The sidecar's operations go to the kernel the first time they are polled
/// and complete whether or not anyone polls them again. Starting several
/// futures and finishing them one at a time therefore runs their I/O side
/// by side.
pub(crate) struct Started<F: Future>(State<F>);

enum State<F: Future> {
    Running(Pin<Box<F>>),
    Done(F::Output),
}

impl<F: Future> Started<F> {
    pub(crate) async fn start(future: F) -> Started<F> {
        let mut future = Box::pin(future);
        let first = std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
        Started(match first {
            Poll::Ready(output) => State::Done(output),
            Poll::Pending => State::Running(future),
        })
    }

    pub(crate) async fn finish(self) -> F::Output {
        match self.0 {
            State::Running(future) => future.await,
            State::Done(output) => output,
        }
    }
}
