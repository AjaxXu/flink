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

package org.apache.flink.table.runtime.over.frame;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.generated.AggsHandleFunction;
import org.apache.flink.table.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.context.ExecutionContext;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.typeutils.BaseRowSerializer;

/**
 * The UnboundedFollowing window frame.
 * UnboundedFollowing窗口frame
 * See {@link RowUnboundedFollowingOverFrame} and {@link RangeUnboundedFollowingOverFrame}.
 */
public abstract class UnboundedFollowingOverFrame implements OverWindowFrame {

	private GeneratedAggsHandleFunction aggsHandleFunction;
	private final RowType valueType;

	private AggsHandleFunction processor;
	private BaseRow accValue;

	/** Rows of the partition currently being processed. */
	ResettableExternalBuffer input;

	private BaseRowSerializer valueSer;

	/**
	 * Index of the first input row with a value equal to or greater than the lower bound of the
	 * current output row.
	 */
	int inputIndex = 0;

	public UnboundedFollowingOverFrame(
			RowType valueType,
			GeneratedAggsHandleFunction aggsHandleFunction) {
		this.valueType = valueType;
		this.aggsHandleFunction = aggsHandleFunction;
	}

	@Override
	public void open(ExecutionContext ctx) throws Exception {
		ClassLoader cl = ctx.getRuntimeContext().getUserCodeClassLoader();
		processor = aggsHandleFunction.newInstance(cl);
		processor.open(new PerKeyStateDataViewStore(ctx.getRuntimeContext()));

		this.aggsHandleFunction = null;
		this.valueSer = new BaseRowSerializer(ctx.getRuntimeContext().getExecutionConfig(), valueType);
	}

	@Override
	public void prepare(ResettableExternalBuffer rows) throws Exception {
		input = rows;
		//cleanup the retired accumulators value
		processor.setAccumulators(processor.createAccumulators());
		inputIndex = 0;
	}

	// 计算从firstRow开始后所有行的累计值
	BaseRow accumulateIterator(
			boolean bufferUpdated,
			BinaryRow firstRow,
			ResettableExternalBuffer.BufferIterator iterator) throws Exception {
		// Only recalculate and update when the buffer changes.
		if (bufferUpdated) {
			//cleanup the retired accumulators value
			processor.setAccumulators(processor.createAccumulators());

			if (firstRow != null) {
				processor.accumulate(firstRow);
			}
			while (iterator.advanceNext()) {
				processor.accumulate(iterator.getRow());
			}
			accValue = valueSer.copy(processor.getValue());
		}
		iterator.close();
		return accValue;
	}
}
