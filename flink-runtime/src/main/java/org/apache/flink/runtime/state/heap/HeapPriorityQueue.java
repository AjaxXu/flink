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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Basic heap-based priority queue for {@link HeapPriorityQueueElement} objects. This heap supports fast deletes
 * because it manages position indexes of the contained {@link HeapPriorityQueueElement}s. The heap implementation is
 * a simple binary tree stored inside an array. Element indexes in the heap array start at 1 instead of 0 to make array
 * index computations a bit simpler in the hot methods. Object identification of remove is based on object identity and
 * not on equals. We use the managed index from {@link HeapPriorityQueueElement} to find an element in the queue
 * array to support fast deletes.
 *
 * <p>Possible future improvements:
 * <ul>
 *  <li>We could also implement shrinking for the heap.</li>
 * </ul>
 *
 * @param <T> type of the contained elements.
 */
public class HeapPriorityQueue<T extends HeapPriorityQueueElement>
	extends AbstractHeapPriorityQueue<T> {

	/**
	 * The index of the head element in the array that represents the heap.
	 */
	private static final int QUEUE_HEAD_INDEX = 1;

	/**
	 * Comparator for the priority of contained elements.
	 */
	@Nonnull
	protected final PriorityComparator<T> elementPriorityComparator;

	/**
	 * Creates an empty {@link HeapPriorityQueue} with the requested initial capacity.
	 *
	 * @param elementPriorityComparator comparator for the priority of contained elements.
	 * @param minimumCapacity the minimum and initial capacity of this priority queue.
	 */
	@SuppressWarnings("unchecked")
	public HeapPriorityQueue(
		@Nonnull PriorityComparator<T> elementPriorityComparator,
		@Nonnegative int minimumCapacity) {
		super(minimumCapacity);
		this.elementPriorityComparator = elementPriorityComparator;
	}

	public void adjustModifiedElement(@Nonnull T element) {
		final int elementIndex = element.getInternalIndex();
		if (element == queue[elementIndex]) {
			adjustElementAtIndex(element, elementIndex);
		}
	}

	@Override
	protected int getHeadElementIndex() {
		return QUEUE_HEAD_INDEX;
	}

	@Override
	protected void addInternal(@Nonnull T element) {
		final int newSize = increaseSizeByOne();
		moveElementToIdx(element, newSize);
		siftUp(newSize);
	}

	@Override
	protected T removeInternal(int removeIdx) {
		T[] heap = this.queue;
		T removedValue = heap[removeIdx];

		// 要删除的idx应该和内部存储value值保存的idx一致
		assert removedValue.getInternalIndex() == removeIdx;

		final int oldSize = size;

		// 删除的不是数组的最后一个元素需要进行位置的调整
		if (removeIdx != oldSize) {
			T element = heap[oldSize];
			// 将原先的最后一个元素放置到要删除的idx处，但是这样的放置没有考虑优先级
			moveElementToIdx(element, removeIdx);
			adjustElementAtIndex(element, removeIdx);
		}

		heap[oldSize] = null;

		--size;
		return removedValue;
	}

	private void adjustElementAtIndex(T element, int index) {
		siftDown(index);
		if (queue[index] == element) {
			siftUp(index);
		}
	}

	private void siftUp(int idx) {
		final T[] heap = this.queue;
		final T currentElement = heap[idx];
		int parentIdx = idx >>> 1;

		// 每次将比较的index，缩小一半，如果被比较元素的优先级高于新插入的元素就将被比较元素后移，直至比较到第一个元素。
		// 这样能够保证idx为1的元素是最早时间触发的
		while (parentIdx > 0 && isElementPriorityLessThen(currentElement, heap[parentIdx])) {
			moveElementToIdx(heap[parentIdx], idx);
			idx = parentIdx;
			parentIdx >>>= 1;
		}

		moveElementToIdx(currentElement, idx);
	}

	private void siftDown(int idx) {
		final T[] heap = this.queue;
		final int heapSize = this.size;

		final T currentElement = heap[idx];
		int firstChildIdx = idx << 1;
		int secondChildIdx = firstChildIdx + 1;

		if (isElementIndexValid(secondChildIdx, heapSize) &&
			isElementPriorityLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
			firstChildIdx = secondChildIdx;
		}

		while (isElementIndexValid(firstChildIdx, heapSize) &&
			isElementPriorityLessThen(heap[firstChildIdx], currentElement)) {
			moveElementToIdx(heap[firstChildIdx], idx);
			idx = firstChildIdx;
			firstChildIdx = idx << 1;
			secondChildIdx = firstChildIdx + 1;

			if (isElementIndexValid(secondChildIdx, heapSize) &&
				isElementPriorityLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
				firstChildIdx = secondChildIdx;
			}
		}

		moveElementToIdx(currentElement, idx);
	}

	private boolean isElementIndexValid(int elementIndex, int heapSize) {
		return elementIndex <= heapSize;
	}

	private boolean isElementPriorityLessThen(T a, T b) {
		return elementPriorityComparator.comparePriority(a, b) < 0;
	}

	private int increaseSizeByOne() {
		final int oldArraySize = queue.length;
		final int minRequiredNewSize = ++size;
		if (minRequiredNewSize >= oldArraySize) {
			final int grow = (oldArraySize < 64) ? oldArraySize + 2 : oldArraySize >> 1;
			// 当存储元素的个数大于数组长度时，需要进行扩容，通过`Arrays.copyOf`进行数组内容的拷贝
			resizeQueueArray(oldArraySize + grow, minRequiredNewSize);
		}
		// TODO implement shrinking as well?
		return minRequiredNewSize;
	}
}
