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
package org.apache.flink.runtime.operators.udf;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 根据最终的样本数据确定范围分区的每个分区的边界
 * Build RangeBoundaries with input records. First, sort the input records, and then select
 * the boundaries with same interval.
 *
 * @param <T>
 */
public class RangeBoundaryBuilder<T> extends RichMapPartitionFunction<T, Object[][]> {

	private int parallelism;
	private final TypeComparatorFactory<T> comparatorFactory;

	public RangeBoundaryBuilder(TypeComparatorFactory<T> comparator, int parallelism) {
		this.comparatorFactory = comparator;
		this.parallelism = parallelism;
	}

	@Override
	public void mapPartition(Iterable<T> values, Collector<Object[][]> out) throws Exception {
		final TypeComparator<T> comparator = this.comparatorFactory.createComparator();
		List<T> sampledData = new ArrayList<>();
		for (T value : values) {
			sampledData.add(value);
		}
		// 第一步对样本进行排序：
		Collections.sort(sampledData, new Comparator<T>() {
			@Override
			public int compare(T first, T second) {
				return comparator.compare(first, second);
			}
		});

		// 第二步采用平均划分法来计算每个分区的边界，边界被存储于一个二维数组中，因为根据样本提取的临界值将会作为比较器的键存储在Object[]中
		int boundarySize = parallelism - 1;
		Object[][] boundaries = new Object[boundarySize][];
		if (sampledData.size() > 0) {
			//计算拆分的段
			double avgRange = sampledData.size() / (double) parallelism;
			int numKey = comparator.getFlatComparators().length;
			//每个并行度（分区）一个边界值
			for (int i = 1; i < parallelism; i++) {
				//计算得到靠近段尾的采样记录作为边界界定标准
				T record = sampledData.get((int) (i * avgRange));
				Object[] keys = new Object[numKey];
				comparator.extractKeys(record, keys, 0);
				boundaries[i-1] = keys;
			}
		}

		out.collect(boundaries);
	}
}
