// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::task::JoinHandle;

use crate::join_error::JoinError;
use crate::join_set::{JoinSet, TaskHandle};
use crate::runtime::RuntimeHandle;
use crate::trace_utils::{trace_block, trace_future};

mod private {
    pub trait Sealed {}
}

/// DataFusion-owned compatibility abstraction for spawning a single task.
///
/// This trait is backed by Tokio today and preserves DataFusion's existing
/// `Send + 'static` single-task spawning semantics by returning [`SpawnedTask`].
/// It is DataFusion-owned and is not currently an external executor plugin
/// point. Its generic methods mean it is not a `dyn Executor` abstraction yet.
/// Runtimes that support `!Send` local tasks need separate abstractions.
pub trait TaskSpawner: private::Sealed {
    /// Spawn an asynchronous task.
    fn spawn<T, R>(&self, task: T) -> SpawnedTask<R>
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send + 'static;

    /// Spawn a blocking task.
    ///
    /// Aborting the task may only prevent it from starting. Once the blocking
    /// task is running, it may continue to run to completion.
    fn spawn_blocking<T, R>(&self, task: T) -> SpawnedTask<R>
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send + 'static;
}

impl private::Sealed for RuntimeHandle {}

impl TaskSpawner for RuntimeHandle {
    fn spawn<T, R>(&self, task: T) -> SpawnedTask<R>
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send + 'static,
    {
        SpawnedTask::spawn_on_runtime(task, self)
    }

    fn spawn_blocking<T, R>(&self, task: T) -> SpawnedTask<R>
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send + 'static,
    {
        SpawnedTask::spawn_blocking_on_runtime(task, self)
    }
}

/// DataFusion-owned compatibility abstraction for spawning fan-out work into a
/// [`JoinSet`].
///
/// This trait is backed by Tokio today and preserves DataFusion's existing
/// `Send + 'static` fan-out spawning semantics by inserting into an existing
/// [`JoinSet`] and returning [`TaskHandle`]. It is DataFusion-owned and is not
/// an external executor plugin point. Runtimes that support `!Send` local tasks
/// need separate abstractions; this trait is not a `!Send` local runtime
/// abstraction.
pub trait JoinSetSpawner: private::Sealed {
    /// Spawn an asynchronous task into an existing join set.
    fn spawn_join_set<T, R>(&self, join_set: &mut JoinSet<R>, task: T) -> TaskHandle
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send + 'static;

    /// Spawn a blocking task into an existing join set.
    ///
    /// Aborting the task may only prevent it from starting. Once the blocking
    /// task is running, it may continue to run to completion.
    fn spawn_blocking_join_set<T, R>(
        &self,
        join_set: &mut JoinSet<R>,
        task: T,
    ) -> TaskHandle
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send + 'static;
}

impl JoinSetSpawner for RuntimeHandle {
    fn spawn_join_set<T, R>(&self, join_set: &mut JoinSet<R>, task: T) -> TaskHandle
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send + 'static,
    {
        join_set.spawn_task_on_runtime(task, self)
    }

    fn spawn_blocking_join_set<T, R>(
        &self,
        join_set: &mut JoinSet<R>,
        task: T,
    ) -> TaskHandle
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send + 'static,
    {
        join_set.spawn_blocking_task_on_runtime(task, self)
    }
}

/// Helper that  provides a simple API to spawn a single task and join it.
/// Provides guarantees of aborting on `Drop` to keep it cancel-safe.
/// Note that if the task was spawned with `spawn_blocking`, it will only be
/// aborted if it hasn't started yet.
///
/// Technically, it's just a wrapper of a `JoinHandle` overriding drop.
#[derive(Debug)]
pub struct SpawnedTask<R> {
    inner: JoinHandle<R>,
}

impl<R: 'static> SpawnedTask<R> {
    pub fn spawn<T>(task: T) -> Self
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn here as SpawnedTask handles aborting/cancelling the task on Drop
        #[expect(clippy::disallowed_methods)]
        let inner = tokio::task::spawn(trace_future(task));
        Self { inner }
    }

    pub fn spawn_blocking<T>(task: T) -> Self
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send,
    {
        // Ok to use spawn_blocking here as SpawnedTask handles aborting/cancelling the task on Drop
        #[expect(clippy::disallowed_methods)]
        let inner = tokio::task::spawn_blocking(trace_block(task));
        Self { inner }
    }

    /// Spawn a task on a provided DataFusion runtime handle.
    pub fn spawn_on_runtime<T>(task: T, handle: &RuntimeHandle) -> Self
    where
        T: Future<Output = R>,
        T: Send + 'static,
        R: Send,
    {
        let inner = handle.as_tokio().spawn(trace_future(task));
        Self { inner }
    }

    /// Spawn a blocking task on a provided DataFusion runtime handle.
    ///
    /// Aborting the task may only prevent it from starting. Once the blocking
    /// task is running, it may continue to run to completion.
    pub fn spawn_blocking_on_runtime<T>(task: T, handle: &RuntimeHandle) -> Self
    where
        T: FnOnce() -> R,
        T: Send + 'static,
        R: Send,
    {
        let inner = handle.as_tokio().spawn_blocking(trace_block(task));
        Self { inner }
    }

    /// Joins the task, returning the result of join (`Result<R, JoinError>`).
    /// Same as awaiting the spawned task, but left for backwards compatibility.
    pub async fn join(self) -> Result<R, JoinError> {
        self.await
    }

    /// Joins the task and unwinds the panic if it happens.
    pub async fn join_unwind(mut self) -> Result<R, JoinError> {
        self.join_unwind_mut().await
    }

    /// Joins the task using a mutable reference and unwinds the panic if it happens.
    ///
    /// This method is similar to [`join_unwind`](Self::join_unwind), but takes a mutable
    /// reference instead of consuming `self`. This allows the `SpawnedTask` to remain
    /// usable after the call.
    ///
    /// If called multiple times on the same task:
    /// - If the task is still running, it will continue waiting for completion
    /// - If the task has already completed successfully, subsequent calls will
    ///   continue to return the same `JoinError` indicating the task is finished
    /// - If the task panicked, the first call will resume the panic, and the
    ///   program will not reach subsequent calls
    pub async fn join_unwind_mut(&mut self) -> Result<R, JoinError> {
        self.await.map_err(|e| {
            // `JoinError` can be caused either by panic or cancellation. We have to handle panics:
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                log::warn!("SpawnedTask was polled during shutdown");
                e
            }
        })
    }
}

impl<R> Future for SpawnedTask<R> {
    type Output = Result<R, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner)
            .poll(cx)
            .map_err(JoinError::from_tokio)
    }
}

impl<R> Drop for SpawnedTask<R> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::{Pending, pending};

    use tokio::{runtime::Runtime, sync::oneshot};

    #[tokio::test]
    async fn runtime_shutdown() {
        let rt = Runtime::new().unwrap();
        #[expect(clippy::async_yields_async)]
        let task = rt
            .spawn(async {
                SpawnedTask::spawn(async {
                    let fut: Pending<()> = pending();
                    fut.await;
                    unreachable!("should never return");
                })
            })
            .await
            .unwrap();

        // caller shutdown their DF runtime (e.g. timeout, error in caller, etc)
        rt.shutdown_background();

        // race condition
        // poll occurs during shutdown (buffered stream poll calls, etc)
        assert!(matches!(
            task.join_unwind().await,
            Err(e) if e.is_cancelled()
        ));
    }

    #[tokio::test]
    #[should_panic(expected = "foo")]
    async fn panic_resume() {
        // this should panic w/o an `unwrap`
        SpawnedTask::spawn(async { panic!("foo") })
            .join_unwind()
            .await
            .ok();
    }

    #[tokio::test]
    async fn cancel_not_started_task() {
        let (sender, receiver) = oneshot::channel::<i32>();
        let task = SpawnedTask::spawn(async {
            // Shouldn't be reached.
            sender.send(42).unwrap();
        });

        drop(task);

        // If the task was cancelled, the sender was also dropped,
        // and awaiting the receiver should result in an error.
        assert!(receiver.await.is_err());
    }

    #[tokio::test]
    async fn cancel_ongoing_task() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let task = SpawnedTask::spawn(async move {
            sender.send(1).await.unwrap();
            // This line will never be reached because the channel has a buffer
            // of 1.
            sender.send(2).await.unwrap();
        });
        // Let the task start.
        assert_eq!(receiver.recv().await.unwrap(), 1);
        drop(task);

        // The sender was dropped so we receive `None`.
        assert!(receiver.recv().await.is_none());
    }

    #[test]
    fn spawn_on_runtime_uses_provided_runtime() {
        let rt = Runtime::new().unwrap();
        let handle = RuntimeHandle::from_tokio(rt.handle().clone());

        let task = SpawnedTask::spawn_on_runtime(async { 42 }, &handle);

        assert_eq!(rt.block_on(task.join()).unwrap(), 42);
    }

    #[test]
    fn spawn_blocking_on_runtime_uses_provided_runtime() {
        let rt = Runtime::new().unwrap();
        let handle = RuntimeHandle::from_tokio(rt.handle().clone());

        let task = SpawnedTask::spawn_blocking_on_runtime(|| 42, &handle);

        assert_eq!(rt.block_on(task.join()).unwrap(), 42);
    }

    #[test]
    fn task_spawner_spawn_uses_provided_runtime() {
        let rt = Runtime::new().unwrap();
        let handle = RuntimeHandle::from_tokio(rt.handle().clone());

        let task = TaskSpawner::spawn(&handle, async { 42 });

        assert_eq!(rt.block_on(task.join()).unwrap(), 42);
    }

    #[test]
    fn task_spawner_spawn_blocking_uses_provided_runtime() {
        let rt = Runtime::new().unwrap();
        let handle = RuntimeHandle::from_tokio(rt.handle().clone());

        let task = TaskSpawner::spawn_blocking(&handle, || 42);

        assert_eq!(rt.block_on(task.join()).unwrap(), 42);
    }

    #[test]
    fn join_set_spawner_spawn_joins_with_task_id() {
        let rt = Runtime::new().unwrap();
        let handle = RuntimeHandle::from_tokio(rt.handle().clone());
        let mut join_set = JoinSet::new();

        let task_handle =
            JoinSetSpawner::spawn_join_set(&handle, &mut join_set, async { 42 });
        let expected_id = task_handle.id();

        let (task_id, value) = rt
            .block_on(join_set.join_next_with_task_id())
            .unwrap()
            .unwrap();
        assert_eq!(task_id, expected_id);
        assert_eq!(value, 42);
        assert!(task_handle.is_finished());
    }

    #[test]
    fn join_set_spawner_spawn_blocking_joins_with_task_id() {
        let rt = Runtime::new().unwrap();
        let handle = RuntimeHandle::from_tokio(rt.handle().clone());
        let mut join_set = JoinSet::new();

        let task_handle =
            JoinSetSpawner::spawn_blocking_join_set(&handle, &mut join_set, || 42);
        let expected_id = task_handle.id();

        let (task_id, value) = rt
            .block_on(join_set.join_next_with_task_id())
            .unwrap()
            .unwrap();
        assert_eq!(task_id, expected_id);
        assert_eq!(value, 42);
        assert!(task_handle.is_finished());
    }
}
