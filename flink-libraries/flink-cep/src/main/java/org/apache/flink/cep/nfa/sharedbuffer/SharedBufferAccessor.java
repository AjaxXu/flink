/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOVICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Vhe ASF licenses this file
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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.flink.cep.nfa.compiler.NFAStateNameHandler.getOriginalNameFromInternal;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Accessor to SharedBuffer that allows operations on the underlying structures in batches.
 * Operations are persisted only after closing the Accessor.
 */
public class SharedBufferAccessor<V> implements AutoCloseable {

	/** The sharedBuffer to store the partial matched events.*/
	private SharedBuffer<V> sharedBuffer;

	SharedBufferAccessor(SharedBuffer<V> sharedBuffer) {
		this.sharedBuffer = sharedBuffer;
	}

	/**
	 * Notifies shared buffer that there will be no events with timestamp &lt;&eq; the given value. It allows to clear
	 * internal counters for number of events seen so far per timestamp.
	 *
	 * @param timestamp watermark, no earlier events will arrive
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public void advanceTime(long timestamp) throws Exception {
		sharedBuffer.advanceTime(timestamp);
	}

	/**
	 * Adds another unique event to the shared buffer and assigns a unique id for it. It automatically creates a
	 * lock on this event, so it won't be removed during processing of that event. Therefore the lock should be removed
	 * after processing all {@link org.apache.flink.cep.nfa.ComputationState}s
	 *
	 * <p><b>NOTE:</b>Should be called only once for each unique event!
	 *
	 * @param value event to be registered
	 * @return unique id of that event that should be used when putting entries to the buffer.
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public EventId registerEvent(V value, long timestamp) throws Exception {
		return sharedBuffer.registerEvent(value, timestamp);
	}

	/**
	 * Stores given value (value + timestamp) under the given state. It assigns a preceding element
	 * relation to the previous entry.
	 *
	 * @param stateName      name of the state that the event should be assigned to
	 * @param eventId        unique id of event assigned by this SharedBuffer
	 * @param previousNodeId id of previous entry (might be null if start of new run)
	 * @param version        Version of the previous relation
	 * @return assigned id of this element
	 */
	public NodeId put(
		final String stateName,
		final EventId eventId,
		@Nullable final NodeId previousNodeId,
		final DeweyNumber version) {

		if (previousNodeId != null) {
			lockNode(previousNodeId);
		}

		NodeId currentNodeId = new NodeId(eventId, getOriginalNameFromInternal(stateName));
		Lockable<SharedBufferNode> currentNode = sharedBuffer.getEntry(currentNodeId);
		if (currentNode == null) {
			currentNode = new Lockable<>(new SharedBufferNode(), 0);
			lockEvent(eventId);
		}

		currentNode.getElement().addEdge(new SharedBufferEdge(
			previousNodeId,
			version));
		sharedBuffer.upsertEntry(currentNodeId, currentNode);

		return currentNodeId;
	}

	/**
	 * Returns all elements from the previous relation starting at the given entry.
	 *
	 * @param nodeId  id of the starting entry
	 * @param version Version of the previous relation which shall be extracted
	 * @return Collection of previous relations starting with the given value
	 */
	public List<Map<String, List<EventId>>> extractPatterns(
		final NodeId nodeId,
		final DeweyNumber version) {

		List<Map<String, List<EventId>>> result = new ArrayList<>();

		// stack to remember the current extraction states
		//构建一个栈来记住当前提取的状态
		Stack<SharedBufferAccessor.ExtractionState> extractionStates = new Stack<>();

		// get the starting shared buffer entry for the previous relation
		// 获得首个共享缓冲区项
		Lockable<SharedBufferNode> entryLock = sharedBuffer.getEntry(nodeId);

		//如果记录项存在
		if (entryLock != null) {
			SharedBufferNode entry = entryLock.getElement();
			//根据记录项，首先构建一个提取状态加入栈
			extractionStates.add(new SharedBufferAccessor.ExtractionState(Tuple2.of(nodeId, entry), version, new Stack<>()));

			// use a depth first search to reconstruct the previous relations
			//当提取状态的栈不为空时，使用深度优先的搜索来重构之前的关系
			while (!extractionStates.isEmpty()) {
				//出栈一个对象
				final SharedBufferAccessor.ExtractionState extractionState = extractionStates.pop();
				// current path of the depth first search
				//获得其栈来存储当前路径，深度优先搜索
				final Stack<Tuple2<NodeId, SharedBufferNode>> currentPath = extractionState.getPath();
				final Tuple2<NodeId, SharedBufferNode> currentEntry = extractionState.getEntry();

				// termination criterion
				//终止条件：某个提取状态为null，说明深度搜索已到达头状态
				if (currentEntry == null) {
					final Map<String, List<EventId>> completePath = new LinkedHashMap<>();

					//出栈构建正向的完整路径存储到LinkedHashMap中，并加入到结果集
					while (!currentPath.isEmpty()) {
						final NodeId currentPathEntry = currentPath.pop().f0;

						String page = currentPathEntry.getPageName();
						List<EventId> values = completePath
							.computeIfAbsent(page, k -> new ArrayList<>());
						values.add(currentPathEntry.getEventId());
					}
					result.add(completePath);
				} else {

					// append state to the path
					//追加到路径中
					currentPath.push(currentEntry);

					boolean firstMatch = true;
					//从当前记录项开始探索与其关联的边，检测版本是否兼容
					for (SharedBufferEdge edge : currentEntry.f1.getEdges()) {
						// we can only proceed if the current version is compatible to the version
						// of this previous relation
						final DeweyNumber currentVersion = extractionState.getVersion();
						// 如果版本号兼容
						if (currentVersion.isCompatibleWith(edge.getDeweyNumber())) {
							final NodeId target = edge.getTarget();
							Stack<Tuple2<NodeId, SharedBufferNode>> newPath;

							//首次匹配，构建提取状态并直接加入栈中，后续匹配需要为提取状态构建新的路径栈，通过深度拷贝路径
							//因为除了首次匹配路径唯一之外，后续的匹配路径都可能不一致，因此不能共享状态
							if (firstMatch) {
								// for the first match we don't have to copy the current path
								newPath = currentPath;
								firstMatch = false;
							} else {
								newPath = new Stack<>();
								newPath.addAll(currentPath);
							}

							extractionStates.push(new SharedBufferAccessor.ExtractionState(
								target != null ? Tuple2.of(target, sharedBuffer.getEntry(target).getElement()) : null,
								edge.getDeweyNumber(),
								newPath));
						}
					}
				}

			}
		}
		return result;
	}

	/**
	 * Extracts the real event from the sharedBuffer with pre-extracted eventId.
	 *
	 * @param match the matched event's eventId.
	 * @return the event associated with the eventId.
	 */
	public Map<String, List<V>> materializeMatch(Map<String, List<EventId>> match) {
		Map<String, List<V>> materializedMatch = new LinkedHashMap<>(match.size());

		for (Map.Entry<String, List<EventId>> pattern : match.entrySet()) {
			List<V> events = new ArrayList<>(pattern.getValue().size());
			for (EventId eventId : pattern.getValue()) {
				try {
					V event = sharedBuffer.getEvent(eventId).getElement();
					events.add(event);
				} catch (Exception ex) {
					throw new WrappingRuntimeException(ex);
				}
			}
			materializedMatch.put(pattern.getKey(), events);
		}

		return materializedMatch;
	}

	/**
	 * Increases the reference counter for the given entry so that it is not
	 * accidentally removed.
	 *
	 * @param node id of the entry
	 */
	public void lockNode(final NodeId node) {
		Lockable<SharedBufferNode> sharedBufferNode = sharedBuffer.getEntry(node);
		if (sharedBufferNode != null) {
			sharedBufferNode.lock();
			sharedBuffer.upsertEntry(node, sharedBufferNode);
		}
	}

	/**
	 * Decreases the reference counter for the given entry so that it can be
	 * removed once the reference counter reaches 0.
	 *
	 * @param node id of the entry
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public void releaseNode(final NodeId node) throws Exception {
		Lockable<SharedBufferNode> sharedBufferNode = sharedBuffer.getEntry(node);
		if (sharedBufferNode != null) {
			if (sharedBufferNode.release()) {
				removeNode(node, sharedBufferNode.getElement());
			} else {
				sharedBuffer.upsertEntry(node, sharedBufferNode);
			}
		}
	}

	/**
	 * Removes the {@code SharedBufferNode}, when the ref is decreased to zero, and also
	 * decrease the ref of the edge on this node.
	 *
	 * @param node id of the entry
	 * @param sharedBufferNode the node body to be removed
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	private void removeNode(NodeId node, SharedBufferNode sharedBufferNode) throws Exception {
		sharedBuffer.removeEntry(node);
		EventId eventId = node.getEventId();
		releaseEvent(eventId);

		for (SharedBufferEdge sharedBufferEdge : sharedBufferNode.getEdges()) {
			releaseNode(sharedBufferEdge.getTarget());
		}
	}

	/**
	 * Increases the reference counter for the given event so that it is not
	 * accidentally removed.
	 *
	 * @param eventId id of the entry
	 */
	private void lockEvent(EventId eventId) {
		Lockable<V> eventWrapper = sharedBuffer.getEvent(eventId);
		checkState(
			eventWrapper != null,
			"Referring to non existent event with id %s",
			eventId);
		eventWrapper.lock();
		sharedBuffer.upsertEvent(eventId, eventWrapper);
	}

	/**
	 * Decreases the reference counter for the given event so that it can be
	 * removed once the reference counter reaches 0.
	 *
	 * @param eventId id of the event
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public void releaseEvent(EventId eventId) throws Exception {
		Lockable<V> eventWrapper = sharedBuffer.getEvent(eventId);
		if (eventWrapper != null) {
			if (eventWrapper.release()) {
				sharedBuffer.removeEvent(eventId);
			} else {
				sharedBuffer.upsertEvent(eventId, eventWrapper);
			}
		}
	}

	/**
	 * Persists the entry in the cache to the underlay state.
	 *
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public void close() throws Exception {
		sharedBuffer.flushCache();
	}

	/**
	 * Helper class to store the extraction state while extracting a sequence of values following
	 * the versioned entry edges.
	 */
	private static class ExtractionState {

		private final Tuple2<NodeId, SharedBufferNode> entry;
		private final DeweyNumber version;
		private final Stack<Tuple2<NodeId, SharedBufferNode>> path;

		ExtractionState(
			final Tuple2<NodeId, SharedBufferNode> entry,
			final DeweyNumber version,
			final Stack<Tuple2<NodeId, SharedBufferNode>> path) {
			this.entry = entry;
			this.version = version;
			this.path = path;
		}

		public Tuple2<NodeId, SharedBufferNode> getEntry() {
			return entry;
		}

		public Stack<Tuple2<NodeId, SharedBufferNode>> getPath() {
			return path;
		}

		public DeweyNumber getVersion() {
			return version;
		}

		@Override
		public String toString() {
			return "ExtractionState(" + entry + ", " + version + ", [" +
				StringUtils.join(path, ", ") + "])";
		}
	}
}
