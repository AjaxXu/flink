/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.config.OptimizerConfigOptions;

/**
 * Aggregate phase strategy which could be specified in {@link OptimizerConfigOptions#SQL_OPTIMIZER_AGG_PHASE_STRATEGY}.
 */
public enum AggregatePhaseStrategy {

	/**
	 * No special enforcer for aggregate stage. Whether to choose two stage aggregate or one stage aggregate depends on cost.
	 * 是选择两阶段聚合还是一阶段聚合取决于成本。
	 */
	AUTO,

	/**
	 * Enforce to use one stage aggregate which only has CompleteGlobalAggregate.
	 * 强制使用只有CompleteGlobalAggregate的一个阶段聚合。
	 */
	ONE_PHASE,

	/**
	 * Enforce to use two stage aggregate which has localAggregate and globalAggregate.
	 * NOTE: If aggregate call does not support split into two phase, still use one stage aggregate.
	 * 强制使用具有localAggregate和globalAggregate的两阶段聚合。
	 * 注意：如果聚合调用不支持拆分为两个阶段，仍然使用一个阶段聚合。
	 */
	TWO_PHASE
}
