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

use tokio::runtime::{Handle, TryCurrentError};

use std::error::Error;
use std::fmt::{Display, Formatter};

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

/// An owned handle to an async runtime used by DataFusion runtime utilities.
///
/// This type keeps new DataFusion APIs from requiring callers to pass Tokio's
/// runtime handle directly. Existing APIs that accept [`Handle`] remain
/// available for compatibility.
#[derive(Clone, Debug)]
pub struct RuntimeHandle {
    inner: Handle,
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

    /// Creates a DataFusion runtime handle from a Tokio runtime handle.
    pub fn from_tokio(handle: Handle) -> Self {
        Self { inner: handle }
    }

    pub(crate) fn as_tokio(&self) -> &Handle {
        &self.inner
    }
}
