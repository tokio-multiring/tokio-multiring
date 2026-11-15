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
