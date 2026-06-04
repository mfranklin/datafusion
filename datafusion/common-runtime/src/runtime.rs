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

use futures::future::BoxFuture;
use tokio::runtime::{Handle, RuntimeFlavor, TryCurrentError};

use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::JoinError;

/// Error returned when a handle to the current runtime is unavailable.
#[derive(Debug)]
pub struct TryCurrentRuntimeError {
    inner: TryCurrentError,
}

impl TryCurrentRuntimeError {
    fn from_tokio(inner: TryCurrentError) -> Self {
        Self { inner }
    }
}

impl Display for TryCurrentRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error for TryCurrentRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.inner)
    }
}

/// Public abstraction used by DataFusion to spawn execution work.
///
/// Implementations may be backed by Tokio or by another executor. DataFusion's
/// execution tasks currently require `Send + 'static` futures and blocking
/// closures. [`spawn_blocking`](Self::spawn_blocking) is used for work that may
/// block or consume CPU for long periods.
pub trait RuntimeSpawner: Send + Sync + 'static {
    /// Spawn an asynchronous task.
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> SpawnHandle;

    /// Spawn blocking or CPU-heavy work.
    fn spawn_blocking(&self, task: Box<dyn FnOnce() + Send + 'static>) -> SpawnHandle;

    /// Return whether spawning can run concurrently with the caller.
    fn is_multi_thread(&self) -> bool {
        true
    }
}

/// Awaitable handle returned by [`RuntimeSpawner`].
pub struct SpawnHandle {
    inner: BoxFuture<'static, Result<(), JoinError>>,
    abort: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl SpawnHandle {
    /// Create a new spawn handle from an awaitable join future and abort callback.
    pub fn new(
        inner: BoxFuture<'static, Result<(), JoinError>>,
        abort: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner,
            abort: Some(Arc::new(abort)),
        }
    }

    /// Request cancellation of the spawned task.
    pub fn abort(&self) {
        if let Some(abort) = &self.abort {
            abort();
        }
    }

    pub(crate) fn abort_handle(&self) -> Arc<dyn Fn() + Send + Sync> {
        self.abort
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::new(|| {}))
    }
}

impl Future for SpawnHandle {
    type Output = Result<(), JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl Drop for SpawnHandle {
    fn drop(&mut self) {
        if let Some(abort) = self.abort.take() {
            abort();
        }
    }
}

struct TokioRuntimeSpawner {
    handle: Handle,
}

impl RuntimeSpawner for TokioRuntimeSpawner {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> SpawnHandle {
        let handle = self.handle.spawn(fut);
        let abort = handle.abort_handle();
        SpawnHandle::new(
            Box::pin(async move { handle.await.map_err(JoinError::from_tokio) }),
            move || {
                abort.abort();
            },
        )
    }

    fn spawn_blocking(&self, task: Box<dyn FnOnce() + Send + 'static>) -> SpawnHandle {
        let handle = self.handle.spawn_blocking(task);
        let abort = handle.abort_handle();
        SpawnHandle::new(
            Box::pin(async move { handle.await.map_err(JoinError::from_tokio) }),
            move || {
                abort.abort();
            },
        )
    }

    fn is_multi_thread(&self) -> bool {
        self.handle.runtime_flavor() == RuntimeFlavor::MultiThread
    }
}

/// An owned handle to a runtime used by DataFusion runtime utilities.
///
/// This DataFusion-owned wrapper keeps cross-thread `Send` task APIs from
/// requiring callers to pass Tokio's runtime handle directly. It can be backed
/// by Tokio or by a custom [`RuntimeSpawner`]. Existing APIs that accept
/// [`Handle`] remain available for compatibility.
#[derive(Clone)]
pub struct RuntimeHandle {
    spawner: Arc<dyn RuntimeSpawner>,
    tokio: Option<Handle>,
}

impl RuntimeHandle {
    /// Returns a handle to the runtime currently running on this thread.
    ///
    /// # Panics
    ///
    /// Panics if called from outside a Tokio runtime.
    pub fn current() -> Self {
        Self::from_tokio(Handle::current())
    }

    /// Attempts to return a handle to the runtime currently running on this thread.
    pub fn try_current() -> Result<Self, TryCurrentRuntimeError> {
        Handle::try_current()
            .map(Self::from_tokio)
            .map_err(TryCurrentRuntimeError::from_tokio)
    }

    /// Returns whether this handle is backed by a multi-threaded runtime.
    ///
    /// This reflects DataFusion's current Tokio-backed runtime implementation.
    pub fn is_multi_thread(&self) -> bool {
        self.spawner.is_multi_thread()
    }

    /// Creates a DataFusion runtime handle from a Tokio runtime handle.
    pub fn from_tokio(handle: Handle) -> Self {
        Self {
            spawner: Arc::new(TokioRuntimeSpawner {
                handle: handle.clone(),
            }),
            tokio: Some(handle),
        }
    }

    /// Creates a DataFusion runtime handle from a custom spawner.
    pub fn from_spawner(spawner: Arc<dyn RuntimeSpawner>) -> Self {
        Self {
            spawner,
            tokio: None,
        }
    }

    pub(crate) fn spawner(&self) -> &dyn RuntimeSpawner {
        self.spawner.as_ref()
    }

    pub(crate) fn is_tokio(&self) -> bool {
        self.tokio.is_some()
    }

    pub(crate) fn as_tokio(&self) -> &Handle {
        self.tokio
            .as_ref()
            .expect("RuntimeHandle is not backed by Tokio")
    }
}

impl Debug for RuntimeHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeHandle")
            .field("is_tokio", &self.tokio.is_some())
            .field("is_multi_thread", &self.is_multi_thread())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeHandle;
    use tokio::runtime::Builder;

    #[test]
    fn is_multi_thread_returns_false_for_current_thread_runtime() {
        let runtime = Builder::new_current_thread().build().unwrap();
        let handle = RuntimeHandle::from_tokio(runtime.handle().clone());

        assert!(!handle.is_multi_thread());
    }

    #[test]
    fn is_multi_thread_returns_true_for_multi_thread_runtime() {
        let runtime = Builder::new_multi_thread().build().unwrap();
        let handle = RuntimeHandle::from_tokio(runtime.handle().clone());

        assert!(handle.is_multi_thread());
    }
}
