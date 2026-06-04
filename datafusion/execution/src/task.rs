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

use crate::{
    config::SessionConfig, memory_pool::MemoryPool, registry::FunctionRegistry,
    runtime_env::RuntimeEnv,
};
use datafusion_common::{Result, internal_datafusion_err, plan_datafusion_err};
use datafusion_common_runtime::RuntimeHandle;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{AggregateUDF, HigherOrderUDF, ScalarUDF, WindowUDF};
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};

/// Task Execution Context
///
/// A [`TaskContext`] contains the state required during a single query's
/// execution. Please see the documentation on [`SessionContext`] for more
/// information.
///
/// # Relationship with [`ExecutionProps`]
///
/// [`TaskContext`] is intentionally distinct from [`ExecutionProps`].
/// [`ExecutionProps`] is state used while optimizing a logical
/// plan and constructing a physical plan.
///
/// [`TaskContext`] is the runtime context passed to physical operators when
/// executing a physical plan. It carries runtime services and session state
/// needed at that stage, such as [`RuntimeEnv`], memory-pool access, session
/// configuration, and function lookup.
///
/// Keeping these structures separate avoids threading execution/runtime state
/// through planning APIs, and avoids making execution depend on planner-only
/// scratch state.
///
/// [`SessionContext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
/// [`ExecutionProps`]: datafusion_expr::execution_props::ExecutionProps
#[derive(Debug)]
pub struct TaskContext {
    /// Session Id
    session_id: String,
    /// Optional Task Identify
    task_id: Option<String>,
    /// Session configuration
    session_config: SessionConfig,
    /// Scalar functions associated with this task context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Higher order functions associated with this task context
    higher_order_functions: HashMap<String, Arc<HigherOrderUDF>>,
    /// Aggregate functions associated with this task context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Window functions associated with this task context
    window_functions: HashMap<String, Arc<WindowUDF>>,
    /// Runtime environment associated with this task context
    runtime: Arc<RuntimeEnv>,
    /// Runtime handle for spawning execution tasks, if available
    runtime_handle: Option<RuntimeHandle>,
}

impl Default for TaskContext {
    fn default() -> Self {
        let runtime = Arc::new(RuntimeEnv::default());

        // Create a default task context, mostly useful for testing
        Self {
            session_id: "DEFAULT".to_string(),
            task_id: None,
            session_config: SessionConfig::new(),
            scalar_functions: HashMap::new(),
            higher_order_functions: HashMap::new(),
            aggregate_functions: HashMap::new(),
            window_functions: HashMap::new(),
            runtime,
            runtime_handle: RuntimeHandle::try_current().ok(),
        }
    }
}

impl TaskContext {
    /// Create a new [`TaskContext`] instance.
    ///
    /// Most users will use [`SessionContext::task_ctx`] to create [`TaskContext`]s
    ///
    /// [`SessionContext::task_ctx`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html#method.task_ctx
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        task_id: Option<String>,
        session_id: String,
        session_config: SessionConfig,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        higher_order_functions: HashMap<String, Arc<HigherOrderUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        window_functions: HashMap<String, Arc<WindowUDF>>,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        Self::new_with_runtime_handle(
            task_id,
            session_id,
            session_config,
            scalar_functions,
            higher_order_functions,
            aggregate_functions,
            window_functions,
            runtime,
            None,
        )
    }

    /// Create a new [`TaskContext`] instance with an explicit runtime handle.
    ///
    /// The handle is optional because task contexts can be constructed outside
    /// a running async runtime. Operators that need to spawn work can use the
    /// handle when present and fall back to non-spawned execution otherwise.
    #[expect(clippy::too_many_arguments)]
    pub fn new_with_runtime_handle(
        task_id: Option<String>,
        session_id: String,
        session_config: SessionConfig,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        higher_order_functions: HashMap<String, Arc<HigherOrderUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        window_functions: HashMap<String, Arc<WindowUDF>>,
        runtime: Arc<RuntimeEnv>,
        runtime_handle: Option<RuntimeHandle>,
    ) -> Self {
        Self {
            task_id,
            session_id,
            session_config,
            scalar_functions,
            higher_order_functions,
            aggregate_functions,
            window_functions,
            runtime,
            runtime_handle,
        }
    }

    /// Return the SessionConfig associated with this [TaskContext]
    pub fn session_config(&self) -> &SessionConfig {
        &self.session_config
    }

    /// Return the `session_id` of this [TaskContext]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return the `task_id` of this [TaskContext]
    pub fn task_id(&self) -> Option<String> {
        self.task_id.clone()
    }

    /// Return the [`MemoryPool`] associated with this [TaskContext]
    pub fn memory_pool(&self) -> &Arc<dyn MemoryPool> {
        &self.runtime.memory_pool
    }

    /// Return the [RuntimeEnv] associated with this [TaskContext]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        Arc::clone(&self.runtime)
    }

    /// Return the runtime handle associated with this [TaskContext], if available.
    pub fn runtime_handle(&self) -> Option<&RuntimeHandle> {
        self.runtime_handle.as_ref()
    }

    pub fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar_functions
    }

    pub fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        &self.higher_order_functions
    }

    pub fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate_functions
    }

    pub fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        &self.window_functions
    }

    /// Update the [`SessionConfig`]
    pub fn with_session_config(mut self, session_config: SessionConfig) -> Self {
        self.session_config = session_config;
        self
    }

    /// Update the [`RuntimeEnv`]
    pub fn with_runtime(mut self, runtime: Arc<RuntimeEnv>) -> Self {
        self.runtime = runtime;
        self
    }

    /// Update the runtime handle used for spawning execution tasks.
    pub fn with_runtime_handle(mut self, runtime_handle: RuntimeHandle) -> Self {
        self.runtime_handle = Some(runtime_handle);
        self
    }
}

impl FunctionRegistry for TaskContext {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDF named \"{name}\" in the TaskContext")
        })
    }

    fn higher_order_function(&self, name: &str) -> Result<Arc<HigherOrderUDF>> {
        let result = self.higher_order_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!(
                "There is no higher-order function named \"{name}\" in the TaskContext"
            )
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            plan_datafusion_err!("There is no UDAF named \"{name}\" in the TaskContext")
        })
    }

    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        let result = self.window_functions.get(name);

        result.cloned().ok_or_else(|| {
            internal_datafusion_err!(
                "There is no UDWF named \"{name}\" in the TaskContext"
            )
        })
    }
    fn register_udaf(
        &mut self,
        udaf: Arc<AggregateUDF>,
    ) -> Result<Option<Arc<AggregateUDF>>> {
        udaf.aliases().iter().for_each(|alias| {
            self.aggregate_functions
                .insert(alias.clone(), Arc::clone(&udaf));
        });
        Ok(self.aggregate_functions.insert(udaf.name().into(), udaf))
    }
    fn register_udwf(&mut self, udwf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>> {
        udwf.aliases().iter().for_each(|alias| {
            self.window_functions
                .insert(alias.clone(), Arc::clone(&udwf));
        });
        Ok(self.window_functions.insert(udwf.name().into(), udwf))
    }
    fn register_udf(&mut self, udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>> {
        udf.aliases().iter().for_each(|alias| {
            self.scalar_functions
                .insert(alias.clone(), Arc::clone(&udf));
        });
        Ok(self.scalar_functions.insert(udf.name().into(), udf))
    }

    fn register_higher_order_function(
        &mut self,
        function: Arc<HigherOrderUDF>,
    ) -> Result<Option<Arc<HigherOrderUDF>>> {
        function.aliases().iter().for_each(|alias| {
            self.higher_order_functions
                .insert(alias.clone(), Arc::clone(&function));
        });
        Ok(self
            .higher_order_functions
            .insert(function.name().into(), function))
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }

    fn higher_order_function_names(&self) -> HashSet<String> {
        self.higher_order_functions.keys().cloned().collect()
    }

    fn udafs(&self) -> HashSet<String> {
        self.aggregate_functions.keys().cloned().collect()
    }

    fn udwfs(&self) -> HashSet<String> {
        self.window_functions.keys().cloned().collect()
    }
}

/// Produce the [`TaskContext`].
pub trait TaskContextProvider {
    fn task_ctx(&self) -> Arc<TaskContext>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{
        config::{ConfigExtension, ConfigOptions, Extensions},
        extensions_options,
    };

    extensions_options! {
        struct TestExtension {
            value: usize, default = 42
            option_value: Option<usize>, default = None
        }
    }

    impl ConfigExtension for TestExtension {
        const PREFIX: &'static str = "test";
    }

    #[test]
    fn task_context_extensions() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let mut extensions = Extensions::new();
        extensions.insert(TestExtension::default());

        let mut config = ConfigOptions::new().with_extensions(extensions);
        config.set("test.value", "24")?;
        config.set("test.option_value", "42")?;
        let session_config = SessionConfig::from(config);

        let task_context = TaskContext::new(
            Some("task_id".to_string()),
            "session_id".to_string(),
            session_config,
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            runtime,
        );

        let test = task_context
            .session_config()
            .options()
            .extensions
            .get::<TestExtension>();
        assert!(test.is_some());

        assert_eq!(test.unwrap().value, 24);
        assert_eq!(test.unwrap().option_value, Some(42));

        Ok(())
    }

    #[test]
    fn task_context_extensions_default() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let mut extensions = Extensions::new();
        extensions.insert(TestExtension::default());

        let config = ConfigOptions::new().with_extensions(extensions);
        let session_config = SessionConfig::from(config);

        let task_context = TaskContext::new(
            Some("task_id".to_string()),
            "session_id".to_string(),
            session_config,
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            runtime,
        );

        let test = task_context
            .session_config()
            .options()
            .extensions
            .get::<TestExtension>();
        assert!(test.is_some());

        assert_eq!(test.unwrap().value, 42);
        assert_eq!(test.unwrap().option_value, None);

        Ok(())
    }

    #[test]
    fn task_context_runtime_handle_is_explicit() {
        let runtime = Arc::new(RuntimeEnv::default());
        let task_context = TaskContext::new(
            None,
            "session_id".to_string(),
            SessionConfig::new(),
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            Arc::clone(&runtime),
        );
        assert!(task_context.runtime_handle().is_none());

        let tokio_runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        let runtime_handle = RuntimeHandle::from_tokio(tokio_runtime.handle().clone());
        let task_context = TaskContext::new_with_runtime_handle(
            None,
            "session_id".to_string(),
            SessionConfig::new(),
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            HashMap::default(),
            runtime,
            Some(runtime_handle),
        );

        assert!(task_context.runtime_handle().is_some());
    }

    #[test]
    fn task_context_default_uses_current_runtime_when_available() {
        let task_context = TaskContext::default();
        assert!(task_context.runtime_handle().is_none());

        let tokio_runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
        tokio_runtime.block_on(async {
            let task_context = TaskContext::default();
            assert!(task_context.runtime_handle().is_some());
        });
    }
}
