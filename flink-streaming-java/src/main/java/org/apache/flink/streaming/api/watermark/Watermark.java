/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.watermark;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;

/**
 * A Watermark告诉operators，没有时间戳大于或等于watermark时间戳的元素会到达该operator
 * Watermark在源处发射并通过拓扑的operators传播。
 * Operators必须使用{@link org.apache.flink.streaming.api.operators.Output＃emitsWatermark（Watermark）}向下游Operators发放Watermark。
 * 不在内部缓冲元素的operators总是可以转发它们收到的Watermark。
 * 缓冲元素的operators（例如窗口operators）必须在发出由到达的watermark触发的元素之后再转发Watermark。
 *
 * A Watermark tells operators that no elements with a timestamp older or equal
 * to the watermark timestamp should arrive at the operator. Watermarks are emitted at the
 * sources and propagate through the operators of the topology. Operators must themselves emit
 * watermarks to downstream operators using
 * {@link org.apache.flink.streaming.api.operators.Output#emitWatermark(Watermark)}. Operators that
 * do not internally buffer elements can always forward the watermark that they receive. Operators
 * that buffer elements, such as window operators, must forward a watermark after emission of
 * elements that is triggered by the arriving watermark.
 *
 * 在某些情况下，watermark只是一种启发式算法，operators应该能够处理后期元素。
 * 他们可以丢弃这些或更新结果并向下游operators发出更新/撤消。
 * <p>In some cases a watermark is only a heuristic and operators should be able to deal with
 * late elements. They can either discard those or update the result and emit updates/retractions
 * to downstream operations.
 *
 * 当一个源关闭时，它将发出一个带有时间戳{@code Long.MAX_VALUE}的最终watermark。 当operator收到它时，它将知道将来不再有输入。
 * <p>When a source closes it will emit a final watermark with timestamp {@code Long.MAX_VALUE}.
 * When an operator receives this it will know that no more input will be arriving in the future.
 */
@PublicEvolving
public final class Watermark extends StreamElement {

	/** The watermark that signifies end-of-event-time. */
	public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);

	// ------------------------------------------------------------------------

	/** The timestamp of the watermark in milliseconds. */
	private final long timestamp;

	/**
	 * Creates a new watermark with the given timestamp in milliseconds.
	 */
	public Watermark(long timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * Returns the timestamp associated with this {@link Watermark} in milliseconds.
	 */
	public long getTimestamp() {
		return timestamp;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		return this == o ||
				o != null && o.getClass() == Watermark.class && ((Watermark) o).timestamp == this.timestamp;
	}

	@Override
	public int hashCode() {
		return (int) (timestamp ^ (timestamp >>> 32));
	}

	@Override
	public String toString() {
		return "Watermark @ " + timestamp;
	}
}
