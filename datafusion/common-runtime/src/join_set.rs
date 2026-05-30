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

use crate::join_error::JoinError;
use crate::trace_utils::{trace_block, trace_future};
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::task::{AbortHandle, Id, LocalSet};

/// An opaque identifier for a spawned task.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct TaskId {
    inner: Id,
}

impl TaskId {
    pub(crate) fn from_tokio(inner: Id) -> Self {
        Self { inner }
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

/// An owned permission to request cancellation of a spawned task, without
/// awaiting its completion.
///
/// For blocking tasks, aborting may only prevent the task from starting. Once a
/// blocking task is running, it may continue to run to completion.
#[derive(Clone)]
pub struct TaskHandle {
    inner: AbortHandle,
}

impl TaskHandle {
    pub(crate) fn from_tokio(inner: AbortHandle) -> Self {
        Self { inner }
    }

    /// Request cancellation of the task associated with the handle.
    ///
    /// For blocking tasks, this may only prevent the task from starting. Once a
    /// blocking task is running, it may continue to run to completion.
    pub fn abort(&self) {
        self.inner.abort()
    }

    /// Checks if the task associated with this handle has finished.
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }

    /// Returns an opaque identifier for the task associated with this handle.
    pub fn id(&self) -> TaskId {
        TaskId::from_tokio(self.inner.id())
    }
}

impl Debug for TaskHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle")
            .field("id", &self.id())
            .finish()
    }
}

/// A wrapper around Tokio's JoinSet that forwards all API calls while optionally
/// instrumenting spawned tasks and blocking closures with custom tracing behavior.
/// If no tracer is injected via `trace_utils::set_tracer`, tasks and closures are executed
/// without any instrumentation.
#[derive(Debug)]
pub struct JoinSet<T> {
    inner: tokio::task::JoinSet<T>,
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JoinSet<T> {
    /// [JoinSet::new](tokio::task::JoinSet::new) - Create a new JoinSet.
    pub fn new() -> Self {
        Self {
            inner: tokio::task::JoinSet::new(),
        }
    }

    /// [JoinSet::len](tokio::task::JoinSet::len) - Return the number of tasks.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// [JoinSet::is_empty](tokio::task::JoinSet::is_empty) - Check if the JoinSet is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T: 'static> JoinSet<T> {
    /// [JoinSet::spawn](tokio::task::JoinSet::spawn) - Spawn a new task.
    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn(trace_future(task))
    }

    /// Spawn a new task and return a runtime-neutral handle to it.
    pub fn spawn_task<F>(&mut self, task: F) -> TaskHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        TaskHandle::from_tokio(self.spawn(task))
    }

    /// [JoinSet::spawn_on](tokio::task::JoinSet::spawn_on) - Spawn a task on a provided runtime.
    pub fn spawn_on<F>(&mut self, task: F, handle: &Handle) -> AbortHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn_on(trace_future(task), handle)
    }

    /// Spawn a task on a provided runtime and return a runtime-neutral handle to it.
    pub fn spawn_task_on<F>(&mut self, task: F, handle: &Handle) -> TaskHandle
    where
        F: Future<Output = T>,
        F: Send + 'static,
        T: Send,
    {
        TaskHandle::from_tokio(self.spawn_on(task, handle))
    }

    /// [JoinSet::spawn_local](tokio::task::JoinSet::spawn_local) - Spawn a local task.
    pub fn spawn_local<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T>,
        F: 'static,
    {
        self.inner.spawn_local(task)
    }

    /// Spawn a new local task and return a runtime-neutral handle to it.
    pub fn spawn_local_task<F>(&mut self, task: F) -> TaskHandle
    where
        F: Future<Output = T>,
        F: 'static,
    {
        TaskHandle::from_tokio(self.spawn_local(task))
    }

    /// [JoinSet::spawn_local_on](tokio::task::JoinSet::spawn_local_on) - Spawn a local task on a provided LocalSet.
    pub fn spawn_local_on<F>(&mut self, task: F, local_set: &LocalSet) -> AbortHandle
    where
        F: Future<Output = T>,
        F: 'static,
    {
        self.inner.spawn_local_on(task, local_set)
    }

    /// Spawn a local task on a provided LocalSet and return a runtime-neutral handle to it.
    pub fn spawn_local_task_on<F>(&mut self, task: F, local_set: &LocalSet) -> TaskHandle
    where
        F: Future<Output = T>,
        F: 'static,
    {
        TaskHandle::from_tokio(self.spawn_local_on(task, local_set))
    }

    /// [JoinSet::spawn_blocking](tokio::task::JoinSet::spawn_blocking) - Spawn a blocking task.
    pub fn spawn_blocking<F>(&mut self, f: F) -> AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn_blocking(trace_block(f))
    }

    /// Spawn a blocking task and return a runtime-neutral handle to it.
    ///
    /// Aborting the returned handle may only prevent the task from starting. Once
    /// the blocking task is running, it may continue to run to completion.
    pub fn spawn_blocking_task<F>(&mut self, f: F) -> TaskHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        TaskHandle::from_tokio(self.spawn_blocking(f))
    }

    /// [JoinSet::spawn_blocking_on](tokio::task::JoinSet::spawn_blocking_on) - Spawn a blocking task on a provided runtime.
    pub fn spawn_blocking_on<F>(&mut self, f: F, handle: &Handle) -> AbortHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        self.inner.spawn_blocking_on(trace_block(f), handle)
    }

    /// Spawn a blocking task on a provided runtime and return a runtime-neutral handle to it.
    ///
    /// Aborting the returned handle may only prevent the task from starting. Once
    /// the blocking task is running, it may continue to run to completion.
    pub fn spawn_blocking_task_on<F>(&mut self, f: F, handle: &Handle) -> TaskHandle
    where
        F: FnOnce() -> T,
        F: Send + 'static,
        T: Send,
    {
        TaskHandle::from_tokio(self.spawn_blocking_on(f, handle))
    }

    /// [JoinSet::join_next](tokio::task::JoinSet::join_next) - Await the next completed task.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.inner
            .join_next()
            .await
            .map(|result| result.map_err(JoinError::from_tokio))
    }

    /// [JoinSet::try_join_next](tokio::task::JoinSet::try_join_next) - Try to join the next completed task.
    pub fn try_join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.inner
            .try_join_next()
            .map(|result| result.map_err(JoinError::from_tokio))
    }

    /// [JoinSet::abort_all](tokio::task::JoinSet::abort_all) - Abort all tasks.
    pub fn abort_all(&mut self) {
        self.inner.abort_all()
    }

    /// [JoinSet::detach_all](tokio::task::JoinSet::detach_all) - Detach all tasks.
    pub fn detach_all(&mut self) {
        self.inner.detach_all()
    }

    /// [JoinSet::poll_join_next](tokio::task::JoinSet::poll_join_next) - Poll for the next completed task.
    pub fn poll_join_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, JoinError>>> {
        match self.inner.poll_join_next(cx) {
            Poll::Ready(Some(Ok(value))) => Poll::Ready(Some(Ok(value))),
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(JoinError::from_tokio(error))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// [JoinSet::join_next_with_id](tokio::task::JoinSet::join_next_with_id) - Await the next completed task with its ID.
    pub async fn join_next_with_id(&mut self) -> Option<Result<(Id, T), JoinError>> {
        self.inner
            .join_next_with_id()
            .await
            .map(|result| result.map_err(JoinError::from_tokio))
    }

    /// Await the next completed task with its runtime-neutral ID.
    pub async fn join_next_with_task_id(
        &mut self,
    ) -> Option<Result<(TaskId, T), JoinError>> {
        self.inner.join_next_with_id().await.map(|result| {
            result
                .map(|(id, value)| (TaskId::from_tokio(id), value))
                .map_err(JoinError::from_tokio)
        })
    }

    /// [JoinSet::try_join_next_with_id](tokio::task::JoinSet::try_join_next_with_id) - Try to join the next completed task with its ID.
    pub fn try_join_next_with_id(&mut self) -> Option<Result<(Id, T), JoinError>> {
        self.inner
            .try_join_next_with_id()
            .map(|result| result.map_err(JoinError::from_tokio))
    }

    /// Try to join the next completed task with its runtime-neutral ID.
    pub fn try_join_next_with_task_id(
        &mut self,
    ) -> Option<Result<(TaskId, T), JoinError>> {
        self.inner.try_join_next_with_id().map(|result| {
            result
                .map(|(id, value)| (TaskId::from_tokio(id), value))
                .map_err(JoinError::from_tokio)
        })
    }

    /// [JoinSet::poll_join_next_with_id](tokio::task::JoinSet::poll_join_next_with_id) - Poll for the next completed task with its ID.
    pub fn poll_join_next_with_id(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(Id, T), JoinError>>> {
        match self.inner.poll_join_next_with_id(cx) {
            Poll::Ready(Some(Ok(value))) => Poll::Ready(Some(Ok(value))),
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(JoinError::from_tokio(error))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Poll for the next completed task with its runtime-neutral ID.
    pub fn poll_join_next_with_task_id(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<(TaskId, T), JoinError>>> {
        match self.inner.poll_join_next_with_id(cx) {
            Poll::Ready(Some(Ok((id, value)))) => {
                Poll::Ready(Some(Ok((TaskId::from_tokio(id), value))))
            }
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(JoinError::from_tokio(error))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// [JoinSet::shutdown](tokio::task::JoinSet::shutdown) - Abort all tasks and wait for shutdown.
    pub async fn shutdown(&mut self) {
        self.inner.shutdown().await
    }

    /// [JoinSet::join_all](tokio::task::JoinSet::join_all) - Await all tasks.
    pub async fn join_all(self) -> Vec<T> {
        self.inner.join_all().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::pending;

    #[tokio::test]
    async fn task_handle_aborts_join_set_task() {
        let mut join_set = JoinSet::new();
        let handle = join_set.spawn_task(pending::<()>());

        handle.abort();

        let error = join_set.join_next().await.unwrap().unwrap_err();
        assert!(error.is_cancelled());
    }

    #[tokio::test]
    async fn cancelled_task_error_id_matches_task_handle_id() {
        let mut join_set = JoinSet::new();
        let handle = join_set.spawn_task(pending::<()>());
        let expected_id = handle.id();

        handle.abort();

        let error = join_set
            .join_next_with_task_id()
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.is_cancelled());
        assert_eq!(error.id(), expected_id);
    }

    #[tokio::test]
    async fn panicked_task_error_id_matches_task_handle_id() {
        let mut join_set = JoinSet::new();
        let handle = join_set.spawn_task(async { panic!("boom") });
        let expected_id = handle.id();

        let error = join_set
            .join_next_with_task_id()
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.is_panic());
        assert_eq!(error.id(), expected_id);
    }

    #[tokio::test]
    async fn join_next_with_task_id_returns_runtime_neutral_id() {
        let mut join_set = JoinSet::new();
        let handle = join_set.spawn_task(async { 42 });
        let expected_id = handle.id();

        let (task_id, value) = join_set.join_next_with_task_id().await.unwrap().unwrap();
        assert_eq!(task_id, expected_id);
        assert_eq!(value, 42);
    }
}
