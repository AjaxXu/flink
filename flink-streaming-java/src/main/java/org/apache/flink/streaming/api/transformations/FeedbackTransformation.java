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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * 该转换器用于表示Flink DAG中的一个反馈点（feedback point）。
 * 所谓反馈点，可用于连接一个或者多个StreamTransformation，这些StreamTransformation被称为反馈边（feedback edges）。
 * 处于反馈点下游的operation将可以从反馈点和反馈边获得元素输入。
 *
 * This represents a feedback point in a topology.
 *
 * <p>This is different from how iterations work in batch processing. Once a feedback point is
 * defined you can connect one or several {@code Transformations} as a feedback edges.
 * Operations downstream from the feedback point will receive elements from the input of this
 * feedback point and from the feedback edges.
 *
 * <p>Both the partitioning of the input and the feedback edges is preserved. They can also have
 * differing partitioning strategies. This requires, however, that the parallelism of the feedback
 * {@code Transformations} must match the parallelism of the input
 * {@code Transformation}.
 *
 * <p>The type of the input {@code Transformation} and the feedback
 * {@code Transformation} must match.
 *
 * @param <T> The type of the input elements and the feedback elements.
 */
@Internal
public class FeedbackTransformation<T> extends Transformation<T> {

	private final Transformation<T> input;

	private final List<Transformation<T>> feedbackEdges;

	private final Long waitTime;

	/**
	 * 实例化FeedbackTransformation时，会自动创建一个用于存储反馈边的集合feedbackEdges。
	 * 那么反馈边如何收集呢？FeedbackTransformation通过定义一个实例方法：addFeedbackEdge来进行收集，
	 * 而这里所谓的“收集”就是将下游StreamTransformation的实例加入feedbackEdges集合中（这里可以理解为将两个点建立连接关系，也就形成了边）。
	 * 不过，这里加入的StreamTransformation的实例有一个要求：也就是当前FeedbackTransformation的实例跟待加入StreamTransformation实例的并行度一致。
	 *
	 * 某种程度上，你可以将其类比于pub-sub机制
	 *
	 * Creates a new {@code FeedbackTransformation} from the given input.
	 *
	 * @param input The input {@code Transformation}
	 * @param waitTime The wait time of the feedback operator. After the time expires
	 *                          the operation will close and not receive any more feedback elements.
	 */
	public FeedbackTransformation(Transformation<T> input, Long waitTime) {
		super("Feedback", input.getOutputType(), input.getParallelism());
		this.input = input;
		this.waitTime = waitTime;
		this.feedbackEdges = Lists.newArrayList();
	}

	/**
	 * Returns the input {@code Transformation} of this {@code FeedbackTransformation}.
	 */
	public Transformation<T> getInput() {
		return input;
	}

	/**
	 * Adds a feedback edge. The parallelism of the {@code Transformation} must match
	 * the parallelism of the input {@code Transformation} of this
	 * {@code FeedbackTransformation}
	 *
	 * @param transform The new feedback {@code Transformation}.
	 */
	public void addFeedbackEdge(Transformation<T> transform) {

		if (transform.getParallelism() != this.getParallelism()) {
			throw new UnsupportedOperationException(
					"Parallelism of the feedback stream must match the parallelism of the original" +
							" stream. Parallelism of original stream: " + this.getParallelism() +
							"; parallelism of feedback stream: " + transform.getParallelism() +
							". Parallelism can be modified using DataStream#setParallelism() method");
		}

		feedbackEdges.add(transform);
	}

	/**
	 * Returns the list of feedback {@code Transformations}.
	 */
	public List<Transformation<T>> getFeedbackEdges() {
		return feedbackEdges;
	}

	/**
	 * Returns the wait time. This is the amount of time that the feedback operator keeps listening
	 * for feedback elements. Once the time expires the operation will close and will not receive
	 * further elements.
	 */
	public Long getWaitTime() {
		return waitTime;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}
}

