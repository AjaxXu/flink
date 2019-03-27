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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.Public;

/**
 * 这是最简单的SinkFunction的实现，它的实现等同于没有实现（其实现为空方法）。
 * 它的作用就是将记录丢弃掉。它的主要场景应该是那些无需最终处理结果的记录
 * A stream sink that ignores all elements.
 *
 * @param <T> The type of elements received by the sink.
 */
@Public
public class DiscardingSink<T> implements SinkFunction<T> {

	private static final long serialVersionUID = 1L;

	@Override
	public void invoke(T value) {}
}
