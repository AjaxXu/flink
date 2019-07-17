/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * 该类仅仅提供passthrough功能，也即直接通过发射器将窗口内的元素迭代发射出去，除此之外不进行任何操作.
 * A {@link AllWindowFunction} that just emits each input element.
 */
@Internal
public class PassThroughAllWindowFunction<W extends Window, T> implements AllWindowFunction<T, T, W> {

	private static final long serialVersionUID = 1L;

	@Override
	public void apply(W window, Iterable<T> input, Collector<T> out) throws Exception {
		for (T in: input) {
			out.collect(in);
		}
	}
}
