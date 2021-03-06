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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.TaskEventHandler;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A basic reader implementation, which wraps an input gate and handles events.
 */
public abstract class AbstractReader implements ReaderBase {

	/** The input gate to read from. */
	protected final InputGate inputGate;

	/** The task event handler to manage task event subscriptions. */
	private final TaskEventHandler taskEventHandler = new TaskEventHandler();

	/** Flag indicating whether this reader allows iteration events. */
	private boolean isIterative;

	/**
	 * The current number of end of superstep events (reset for each superstep). A superstep is
	 * finished after an end of superstep event has been received for each input channel.
	 */
	private int currentNumberOfEndOfSuperstepEvents;

	protected AbstractReader(InputGate inputGate) {
		this.inputGate = inputGate;
	}

	@Override
	public boolean isFinished() {
		return inputGate.isFinished();
	}

	// ------------------------------------------------------------------------
	// Events
	// ------------------------------------------------------------------------

	@Override
	public void registerTaskEventListener(EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType) {
		taskEventHandler.subscribe(listener, eventType);
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		inputGate.sendTaskEvent(event);
	}

	/**
	 * 对读取到的事件提供处理
	 * Handles the event and returns whether the reader reached an end-of-stream event (either the
	 * end of the whole stream or the end of an superstep).
	 */
	protected boolean handleEvent(AbstractEvent event) throws IOException {
		final Class<?> eventType = event.getClass();

		try {
			// ------------------------------------------------------------
			// Runtime events
			// ------------------------------------------------------------

			// This event is also checked at the (single) input gate to release the respective
			// channel, at which it was received.
			// 如果事件为消费完的特定结果子分区中的数据，则直接返回true
			if (eventType == EndOfPartitionEvent.class) {
				return true;
			}
			//如果事件是针对迭代的超步完成，则增加相应的超步完成计数
			else if (eventType == EndOfSuperstepEvent.class) {
				return incrementEndOfSuperstepEventAndCheck();
			}

			// ------------------------------------------------------------
			// Task events (user)
			//如果事件是TaskEvent，则直接用任务事件处理器发布
			// ------------------------------------------------------------
			else if (event instanceof TaskEvent) {
				taskEventHandler.publish((TaskEvent) event);

				return false;
			}
			else {
				throw new IllegalStateException("Received unexpected event of type " + eventType + " at reader.");
			}
		}
		catch (Throwable t) {
			throw new IOException("Error while handling event of type " + eventType + ": " + t.getMessage(), t);
		}
	}

	public void publish(TaskEvent event){
		taskEventHandler.publish(event);
	}

	// ------------------------------------------------------------------------
	// Iterations
	// ------------------------------------------------------------------------

	@Override
	public void setIterativeReader() {
		isIterative = true;
	}

	@Override
	public void startNextSuperstep() {
		checkState(isIterative, "Tried to start next superstep in a non-iterative reader.");
		checkState(currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels(), "Tried to start next superstep before reaching end of previous superstep.");

		currentNumberOfEndOfSuperstepEvents = 0;
	}

	@Override
	public boolean hasReachedEndOfSuperstep() {
		return isIterative && currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels();

	}

	private boolean incrementEndOfSuperstepEventAndCheck() {
		checkState(isIterative, "Tried to increment superstep count in a non-iterative reader.");
		checkState(currentNumberOfEndOfSuperstepEvents + 1 <= inputGate.getNumberOfInputChannels(), "Received too many (" + currentNumberOfEndOfSuperstepEvents + ") end of superstep events.");

		return ++currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels();
	}

}
