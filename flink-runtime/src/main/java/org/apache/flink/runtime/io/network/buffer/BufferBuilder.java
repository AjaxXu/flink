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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Not thread safe class for filling in the content of the {@link MemorySegment}. To access written data please use
 * {@link BufferConsumer} which allows to build {@link Buffer} instances from the written data.
 */
@NotThreadSafe
public class BufferBuilder {
	private final MemorySegment memorySegment;

	private final BufferRecycler recycler;

	private final SettablePositionMarker positionMarker = new SettablePositionMarker();

	private boolean bufferConsumerCreated = false;

	public BufferBuilder(MemorySegment memorySegment, BufferRecycler recycler) {
		this.memorySegment = checkNotNull(memorySegment);
		this.recycler = checkNotNull(recycler);
	}

	/**
	 * @return created matching instance of {@link BufferConsumer} to this {@link BufferBuilder}. There can exist only
	 * one {@link BufferConsumer} per each {@link BufferBuilder} and vice versa.
	 */
	public BufferConsumer createBufferConsumer() {
		checkState(!bufferConsumerCreated, "There can not exists two BufferConsumer for one BufferBuilder");
		bufferConsumerCreated = true;
		return new BufferConsumer(
			memorySegment,
			recycler,
			positionMarker);
	}

	/**
	 * Same as {@link #append(ByteBuffer)} but additionally {@link #commit()} the appending.
	 */
	public int appendAndCommit(ByteBuffer source) {
		int writtenBytes = append(source);
		commit();
		return writtenBytes;
	}

	/**
	 * Append as many data as possible from {@code source}. Not everything might be copied if there is not enough
	 * space in the underlying {@link MemorySegment}
	 *
	 * @return number of copied bytes
	 */
	public int append(ByteBuffer source) {
		checkState(!isFinished());

		int needed = source.remaining();
		int available = getMaxCapacity() - positionMarker.getCached();
		// segment不一定足够大，可能存不下这批buffer, 堆外内存拷贝的时候需要提前计算好可以拷贝的量，否则会有异常
		int toCopy = Math.min(needed, available);

		// 将source buffer中的数据/堆内存，put至memorySegment中，利用Unsafe进行数据拷贝
		memorySegment.put(positionMarker.getCached(), source, toCopy);
		// 设置新的position
		positionMarker.move(toCopy);
		return toCopy;
	}

	/**
	 * Make the change visible to the readers. This is costly operation (volatile access) thus in case of bulk writes
	 * it's better to commit them all together instead one by one.
	 */
	public void commit() {
		positionMarker.commit();
	}

	/**
	 * Mark this {@link BufferBuilder} and associated {@link BufferConsumer} as finished - no new data writes will be
	 * allowed.
	 *
	 * <p>This method should be idempotent to handle failures and task interruptions. Check FLINK-8948 for more details.
	 *
	 * @return number of written bytes.
	 */
	public int finish() {
		int writtenBytes = positionMarker.markFinished();
		commit();
		return writtenBytes;
	}

	public boolean isFinished() {
		return positionMarker.isFinished();
	}

	public boolean isFull() {
		checkState(positionMarker.getCached() <= getMaxCapacity());
		return positionMarker.getCached() == getMaxCapacity();
	}

	public int getMaxCapacity() {
		return memorySegment.size();
	}

	/**
	 * positionMarker会标记生产端的数据写到多少个字节了，这个在消费端的时候也会用到这个position
	 * Holds a reference to the current writer position. Negative values indicate that writer ({@link BufferBuilder}
	 * has finished. Value {@code Integer.MIN_VALUE} represents finished empty buffer.
	 */
	@ThreadSafe
	interface PositionMarker {
		int FINISHED_EMPTY = Integer.MIN_VALUE;

		int get();

		static boolean isFinished(int position) {
			return position < 0;
		}

		static int getAbsolute(int position) {
			if (position == FINISHED_EMPTY) {
				return 0;
			}
			return Math.abs(position);
		}
	}

	/**
	 * Cached writing implementation of {@link PositionMarker}.
	 *
	 * <p>Writer ({@link BufferBuilder}) and reader ({@link BufferConsumer}) caches must be implemented independently
	 * of one another - so that the cached values can not accidentally leak from one to another.
	 *
	 * <p>Remember to commit the {@link SettablePositionMarker} to make the changes visible.
	 */
	private static class SettablePositionMarker implements PositionMarker {
		// 由于是多线程使用所以position的值需要被标记成volatile来保证数据的可见性，每次消费端拉取数据的时候，
		// 对于没有写完的buffer同样可以进行消费,消费前更新一个buffer的position真实位置，这里用到了一个小技巧，由于数据在生产的时候需要频繁的更新position，
		// 如果是volatile的，虽然比较轻量，频繁更新也是比较大的开销，因此加入了一个cachedPosition，在写数据的时候只需要更新builder中的cachedPosition，
		// 生产端每次完成一批的书写才会commit给volatile position，以此来减少缓存刷新.
		private volatile int position = 0;

		/**
		 * Locally cached value of volatile {@code position} to avoid unnecessary volatile accesses.
		 */
		private int cachedPosition = 0;

		@Override
		public int get() {
			return position;
		}

		public boolean isFinished() {
			return PositionMarker.isFinished(cachedPosition);
		}

		public int getCached() {
			return PositionMarker.getAbsolute(cachedPosition);
		}

		/**
		 * Marks this position as finished and returns the current position.
		 * 小于0意味着finished
		 * @return current position as of {@link #getCached()}
		 */
		public int markFinished() {
			int currentPosition = getCached();
			int newValue = -currentPosition;
			if (newValue == 0) {
				newValue = FINISHED_EMPTY;
			}
			set(newValue);
			return currentPosition;
		}

		public void move(int offset) {
			set(cachedPosition + offset);
		}

		public void set(int value) {
			cachedPosition = value;
		}

		public void commit() {
			position = cachedPosition;
		}
	}
}
