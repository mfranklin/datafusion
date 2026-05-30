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

use std::any::Any;
use std::error::Error;
use std::fmt::{Display, Formatter};

use crate::join_set::TaskId;

/// Error returned when joining a spawned task fails.
///
/// This keeps Tokio's join error out of DataFusion's public runtime APIs while
/// preserving the cancellation and panic inspection methods callers use today.
#[derive(Debug)]
pub struct JoinError {
    inner: tokio::task::JoinError,
}

impl JoinError {
    pub(crate) fn from_tokio(inner: tokio::task::JoinError) -> Self {
        Self { inner }
    }

    /// Returns true if the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    /// Returns true if the task panicked.
    pub fn is_panic(&self) -> bool {
        self.inner.is_panic()
    }

    /// Returns an opaque identifier for the task that failed to join.
    pub fn id(&self) -> TaskId {
        TaskId::from_tokio(self.inner.id())
    }

    /// Consumes the error and returns the panic payload.
    ///
    /// # Panics
    ///
    /// Panics if the joined task did not panic.
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        self.inner.into_panic()
    }

    /// Consumes the error and returns the panic payload, if the task panicked.
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, JoinError> {
        self.inner.try_into_panic().map_err(JoinError::from_tokio)
    }
}

impl Display for JoinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error for JoinError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.inner)
    }
}
