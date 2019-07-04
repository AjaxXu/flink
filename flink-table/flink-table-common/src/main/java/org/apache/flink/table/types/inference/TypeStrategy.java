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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;

import java.util.Optional;

/**
 * 推断函数调用的数据类型的策略。推断类型可以描述函数的最终结果或中间结果(累积类型)
 * Strategy for inferring the data type of a function call. The inferred type might describe the
 * final result or an intermediate result (accumulation type) of a function.
 *
 * @see TypeStrategies
 */
@PublicEvolving
public interface TypeStrategy {

	/**
	 * 推断给定函数调用的类型
	 * Infers a type from the given function call.
	 */
	Optional<DataType> inferType(CallContext callContext);
}
