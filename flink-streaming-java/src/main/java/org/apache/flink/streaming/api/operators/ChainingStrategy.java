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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * 该枚举定义了operator的chain strategy(链接策略).当一个operator链接到其前置operator时，
 * 意味着它们将在同一个线程上执行.StreamOperator的默认值是HEAD，这意味着它将没有前置operator，
 * 不过它有可能成为其他operator的前置operator.大部分StreamOperator将该枚举以ALWAYS覆盖，表示它们将链接到一个前置operator
 * Defines the chaining scheme for the operator. When an operator is chained to the
 * predecessor, it means that they run in the same thread. They become one operator
 * consisting of multiple steps.
 *
 * <p>The default value used by the StreamOperator is {@link #HEAD}, which means that
 * the operator is not chained to its predecessor. Most operators override this with
 * {@link #ALWAYS}, meaning they will be chained to predecessors whenever possible.
 */
@PublicEvolving
public enum ChainingStrategy {

	/**
	 * 它允许将当前operator链接到某前置operator，这是提升性能的良好实践，它能够提升operator的并行度
	 * Operators will be eagerly chained whenever possible.
	 *
	 * <p>To optimize performance, it is generally a good practice to allow maximal
	 * chaining and increase operator parallelism.
	 */
	ALWAYS,

	/**
	 * 该策略不支持operator被链接到某前置operator也不支持被作为其他operator的前置operator
	 * The operator will not be chained to the preceding or succeeding operators.
	 */
	NEVER,

	/**
	 * 该策略表示operator没有前置operator，不过可以作为其他operator的chain header
	 * The operator will not be chained to the predecessor, but successors may chain to this
	 * operator.
	 */
	HEAD
}
