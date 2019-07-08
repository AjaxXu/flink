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

package org.apache.flink.table.runtime.deduplicate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Utility for deduplicate function.
 * 去重函数的帮助类
 */
class DeduplicateFunctionHelper {

	/**
	 * Processes element to deduplicate on keys, sends current element as last row, retracts previous element if
	 * needed.
	 * key上去重：已最后一行作为要发送的元素，撤回之前的行(如果需要的话)
	 *
	 * @param currentRow latest row received by deduplicate function
	 * @param generateRetraction whether need to send retract message to downstream
	 * @param state state of function
	 * @param out underlying collector
	 * @throws Exception
	 */
	static void processLastRow(BaseRow currentRow, boolean generateRetraction, ValueState<BaseRow> state,
			Collector<BaseRow> out) throws Exception {
		// Check message should be accumulate
		Preconditions.checkArgument(BaseRowUtil.isAccumulateMsg(currentRow));
		if (generateRetraction) {
			// state stores complete row if generateRetraction is true
			// 支持撤回时，状态保存完整的row
			BaseRow preRow = state.value();
			state.update(currentRow);
			if (preRow != null) {
				// 发出撤回消息
				preRow.setHeader(BaseRowUtil.RETRACT_MSG);
				out.collect(preRow);
			}
		}
		out.collect(currentRow);
	}

	/**
	 * Processes element to deduplicate on keys, sends current element if it is first row.
	 * key上去重，当是第一行时发送，后续相同的key忽略
	 *
	 * @param currentRow latest row received by deduplicate function
	 * @param state state of function
	 * @param out underlying collector
	 * @throws Exception
	 */
	static void processFirstRow(BaseRow currentRow, ValueState<Boolean> state, Collector<BaseRow> out)
			throws Exception {
		// Check message should be accumulate
		Preconditions.checkArgument(BaseRowUtil.isAccumulateMsg(currentRow));
		// ignore record with timestamp bigger than preRow
		// state中的值不为null,直接返回。忽略相同key的后续element
		if (state.value() != null) {
			return;
		}
		state.update(true);
		out.collect(currentRow);
	}

	private DeduplicateFunctionHelper() {

	}
}
