use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use io_uring::squeue;

use crate::reactor::{InFlight, Sidecar};

/// One io_uring operation together with everything the kernel needs kept
/// alive while it runs.
pub(crate) trait Operation: Send + 'static {
    type Output;

    /// Builds the submission. Called once, after `self` has reached its
    /// final address, so the entry may point into `self`.
    fn entry(&mut self) -> squeue::Entry;

    fn finish(self, result: io::Result<u32>) -> Self::Output;
}

enum Stage {
    Pending(Waker),
    Done(i32),
    /// The future was dropped. The operation is kept alive by the reactor
    /// until its completion arrives.
    Abandoned,
}

struct State<T> {
    stage: Stage,
    /// Set once the reactor has queued the submission.
    token: Option<u64>,
    op: Option<T>,
}

struct Inner<T> {
    state: Mutex<State<T>>,
}

impl<T: Operation> InFlight for Inner<T> {
    fn submitted(&self, token: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if matches!(state.stage, Stage::Abandoned) {
            return false;
        }
        state.token = Some(token);
        true
    }

    fn complete(&self, result: i32) {
        let mut state = self.state.lock().unwrap();
        if let Stage::Pending(_) = state.stage {
            let Stage::Pending(waker) = std::mem::replace(&mut state.stage, Stage::Done(result))
            else {
                unreachable!()
            };
            drop(state);
            waker.wake();
        }
    }
}

enum Progress<T> {
    Idle(T),
    InFlight { inner: Arc<Inner<T>>, worker: usize },
    Finished,
}

/// Future of a single operation.
pub(crate) struct Op<T: Operation> {
    sidecar: Sidecar,
    progress: Progress<T>,
}

impl<T: Operation> Op<T> {
    pub(crate) fn new(sidecar: &Sidecar, op: T) -> Self {
        Self {
            sidecar: sidecar.clone(),
            progress: Progress::Idle(op),
        }
    }
}

// The operation is never pinned in place: it is moved into an `Arc` before
// the kernel sees it.
impl<T: Operation> Unpin for Op<T> {}

impl<T: Operation> Future for Op<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T::Output> {
        let this = &mut *self;
        match std::mem::replace(&mut this.progress, Progress::Finished) {
            Progress::Idle(op) => {
                let inner = Arc::new(Inner {
                    state: Mutex::new(State {
                        stage: Stage::Pending(cx.waker().clone()),
                        token: None,
                        op: Some(op),
                    }),
                });
                let entry = inner
                    .state
                    .lock()
                    .unwrap()
                    .op
                    .as_mut()
                    .expect("operation present until finished")
                    .entry();
                let worker = this.sidecar.submit(entry, inner.clone());
                this.progress = Progress::InFlight { inner, worker };
                Poll::Pending
            }
            Progress::InFlight { inner, worker } => {
                let mut state = inner.state.lock().unwrap();
                match &mut state.stage {
                    Stage::Pending(waker) => {
                        waker.clone_from(cx.waker());
                        drop(state);
                        this.progress = Progress::InFlight { inner, worker };
                        Poll::Pending
                    }
                    Stage::Done(result) => {
                        let result = if *result < 0 {
                            Err(io::Error::from_raw_os_error(-*result))
                        } else {
                            Ok(*result as u32)
                        };
                        let op = state.op.take().expect("operation present until finished");
                        Poll::Ready(op.finish(result))
                    }
                    Stage::Abandoned => unreachable!("only set when the future is dropped"),
                }
            }
            Progress::Finished => panic!("operation polled after completion"),
        }
    }
}

impl<T: Operation> Drop for Op<T> {
    fn drop(&mut self) {
        let Progress::InFlight { inner, worker } = &self.progress else {
            return;
        };
        let token = {
            let mut state = inner.state.lock().unwrap();
            if !matches!(state.stage, Stage::Pending(_)) {
                return;
            }
            state.stage = Stage::Abandoned;
            state.token
        };
        // Without a token the submission has not been queued yet, and the
        // reactor will skip it.
        if let Some(token) = token {
            self.sidecar.cancel(*worker, token);
        }
    }
}
