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

package org.apache.flink.table.runtime.over;

import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.JoinedRow;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.TableStreamOperator;
import org.apache.flink.table.runtime.context.ExecutionContextImpl;
import org.apache.flink.table.runtime.over.frame.OverWindowFrame;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.AbstractRowSerializer;

/**
 * the operator for OVER window need cache data by ResettableExternalBuffer for {@link OverWindowFrame}.
 */
public class BufferDataOverWindowOperator extends TableStreamOperator<BaseRow>
		implements OneInputStreamOperator<BaseRow, BaseRow>, BoundedOneInput {

	private final long memorySize;
	private final OverWindowFrame[] overWindowFrames;
	private GeneratedRecordComparator genComparator;
	private final boolean isRowAllInFixedPart;

	private RecordComparator partitionComparator; // 这里叫keyComparator更合适
	private BaseRow lastInput;
	private JoinedRow[] joinedRows;
	private StreamRecordCollector<BaseRow> collector;
	private AbstractRowSerializer<BaseRow> serializer;
	private ResettableExternalBuffer currentData;

	/**
	 * @param memorySize           the memory is assigned to a resettable external buffer.
	 * @param overWindowFrames     the window frames belong to this operator.
	 * @param genComparator       the generated sort which is used for generating the comparator among
	 */
	public BufferDataOverWindowOperator(
			long memorySize,
			OverWindowFrame[] overWindowFrames,
			GeneratedRecordComparator genComparator,
			boolean isRowAllInFixedPart) {
		this.memorySize = memorySize;
		this.overWindowFrames = overWindowFrames;
		this.genComparator = genComparator;
		this.isRowAllInFixedPart = isRowAllInFixedPart;
	}

	@Override
	public void open() throws Exception {
		super.open();

		ClassLoader cl = getUserCodeClassloader();
		serializer = (AbstractRowSerializer) getOperatorConfig().getTypeSerializerIn1(cl);
		partitionComparator = genComparator.newInstance(cl);
		genComparator = null;

		MemoryManager memManager = getContainingTask().getEnvironment().getMemoryManager();
		this.currentData = new ResettableExternalBuffer(
				memManager,
				getContainingTask().getEnvironment().getIOManager(),
				memManager.allocatePages(this, (int) (memorySize / memManager.getPageSize())),
				serializer, isRowAllInFixedPart);

		collector = new StreamRecordCollector<>(output);
		joinedRows = new JoinedRow[overWindowFrames.length];
		for (int i = 0; i < overWindowFrames.length; i++) {
			overWindowFrames[i].open(new ExecutionContextImpl(this, getRuntimeContext()));
			joinedRows[i] = new JoinedRow();
		}
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow input = element.getValue();
		if (lastInput != null && partitionComparator.compare(lastInput, input) != 0) {
			// 说明key已经发生了变化(或者说的partition发生了变化),先处理之前cache的数据
			processCurrentData();
		}
		lastInput = serializer.copy(input);
		currentData.add(lastInput);
	}

	@Override
	public void endInput() throws Exception {
		if (currentData.size() > 0) {
			processCurrentData();
		}
	}

	private void processCurrentData() throws Exception {
		currentData.complete();
		for (OverWindowFrame frame : overWindowFrames) {
			frame.prepare(currentData);
		}
		int rowIndex = 0;
		ResettableExternalBuffer.BufferIterator bufferIterator = currentData.newIterator();
		while (bufferIterator.advanceNext()) {
			BinaryRow currentRow = bufferIterator.getRow();
			BaseRow output = currentRow;
			// TODO Reform AggsHandleFunction.getValue instead of use JoinedRow. Multilayer JoinedRow is slow.
			for (int i = 0; i < overWindowFrames.length; i++) {
				OverWindowFrame frame = overWindowFrames[i];
				BaseRow value = frame.process(rowIndex, currentRow);
				output = joinedRows[i].replace(output, value);
			}
			collector.collect(output);
			rowIndex += 1;
		}
		bufferIterator.close();
		currentData.reset();
	}

	@Override
	public void close() throws Exception {
		super.close();
		this.currentData.close();
	}
}
