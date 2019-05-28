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

import org.apache.flink.configuration.NetworkEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * NetworkBufferPool缓存了固定数目的内存段，主要用于网络栈通信。NetworkBufferPool在每个TaskManager上只有一个，负责所有子task的内存管理
 *
 * 为了方便管理NetworkBuffer，Flink提供了BufferPoolFactory，并且提供了唯一实现NetworkBufferPool，这是个工厂模式的应用
 *
 * The NetworkBufferPool is a fixed size pool of {@link MemorySegment} instances
 * for the network stack.
 *
 * 由于NetworkBufferPool只是个工厂，实际的内存池是LocalBufferPool。
 * 每个TaskManager都只有一个NetworkBufferPool工厂，但是上面运行的每个task都要有一个和其他task隔离的LocalBufferPool池，这从逻辑上很好理解
 *
 * <p>The NetworkBufferPool creates {@link LocalBufferPool}s from which the individual tasks draw
 * the buffers for the network data transfer. When new local buffer pools are created, the
 * NetworkBufferPool dynamically redistributes the buffers between the pools.
 */
public class NetworkBufferPool implements BufferPoolFactory, MemorySegmentProvider {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPool.class);

	private final int totalNumberOfMemorySegments;

	private final int memorySegmentSize;

	private final ArrayBlockingQueue<MemorySegment> availableMemorySegments;

	private volatile boolean isDestroyed;

	// ---- Managed buffer pools ----------------------------------------------

	private final Object factoryLock = new Object();

	private final Set<LocalBufferPool> allBufferPools = new HashSet<>();

	private int numTotalRequiredBuffers;

	private final int numberOfSegmentsToRequest;

	/**
	 * Allocates all {@link MemorySegment} instances managed by this pool.
	 * NetworkBufferPool在构造器的参数中要求指定其缓存的内存段数目，然后它会初始化固定大小的一个队列作为内存段池
	 */
	public NetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize, int numberOfSegmentsToRequest) {

		this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;
		this.memorySegmentSize = segmentSize;

		checkArgument(numberOfSegmentsToRequest > 0, "The number of required buffers should be larger than 0.");
		this.numberOfSegmentsToRequest = numberOfSegmentsToRequest;

		final long sizeInLong = (long) segmentSize;

		try {
			this.availableMemorySegments = new ArrayBlockingQueue<>(numberOfSegmentsToAllocate);
		}
		catch (OutOfMemoryError err) {
			throw new OutOfMemoryError("Could not allocate buffer queue of length "
					+ numberOfSegmentsToAllocate + " - " + err.getMessage());
		}

		try {
			for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
				availableMemorySegments.add(MemorySegmentFactory.allocateUnpooledOffHeapMemory(segmentSize, null));
			}
		}
		catch (OutOfMemoryError err) {
			int allocated = availableMemorySegments.size();

			// free some memory
			availableMemorySegments.clear();

			long requiredMb = (sizeInLong * numberOfSegmentsToAllocate) >> 20;
			long allocatedMb = (sizeInLong * allocated) >> 20;
			long missingMb = requiredMb - allocatedMb;

			throw new OutOfMemoryError("Could not allocate enough memory segments for NetworkBufferPool " +
					"(required (Mb): " + requiredMb +
					", allocated (Mb): " + allocatedMb +
					", missing (Mb): " + missingMb + "). Cause: " + err.getMessage());
		}

		long allocatedMb = (sizeInLong * availableMemorySegments.size()) >> 20;

		LOG.info("Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).",
				allocatedMb, availableMemorySegments.size(), segmentSize);
	}

	@Nullable
	public MemorySegment requestMemorySegment() {
		return availableMemorySegments.poll();
	}

	public void recycle(MemorySegment segment) {
		// Adds the segment back to the queue, which does not immediately free the memory
		// however, since this happens when references to the global pool are also released,
		// making the availableMemorySegments queue and its contained object reclaimable
		availableMemorySegments.add(checkNotNull(segment));
	}

	@Override
	public List<MemorySegment> requestMemorySegments() throws IOException {
		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer pool has already been destroyed.");
			}

			if (numTotalRequiredBuffers + numberOfSegmentsToRequest > totalNumberOfMemorySegments) {
				throw new IOException(String.format("Insufficient number of network buffers: " +
								"required %d, but only %d available. The total number of network " +
								"buffers is currently set to %d of %d bytes each. You can increase this " +
								"number by setting the configuration keys '%s', '%s', and '%s'.",
					numberOfSegmentsToRequest,
						totalNumberOfMemorySegments - numTotalRequiredBuffers,
						totalNumberOfMemorySegments,
						memorySegmentSize,
						NetworkEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key(),
						NetworkEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.key(),
						NetworkEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key()));
			}

			this.numTotalRequiredBuffers += numberOfSegmentsToRequest;

			try {
				redistributeBuffers();
			} catch (Throwable t) {
				this.numTotalRequiredBuffers -= numberOfSegmentsToRequest;

				try {
					redistributeBuffers();
				} catch (IOException inner) {
					t.addSuppressed(inner);
				}
				ExceptionUtils.rethrowIOException(t);
			}
		}

		final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest);
		try {
			while (segments.size() < numberOfSegmentsToRequest) {
				if (isDestroyed) {
					throw new IllegalStateException("Buffer pool is destroyed.");
				}

				final MemorySegment segment = availableMemorySegments.poll(2, TimeUnit.SECONDS);
				if (segment != null) {
					segments.add(segment);
				}
			}
		} catch (Throwable e) {
			try {
				recycleMemorySegments(segments, numberOfSegmentsToRequest);
			} catch (IOException inner) {
				e.addSuppressed(inner);
			}
			ExceptionUtils.rethrowIOException(e);
		}

		return segments;
	}

	@Override
	public void recycleMemorySegments(Collection<MemorySegment> segments) throws IOException {
		recycleMemorySegments(segments, segments.size());
	}

	private void recycleMemorySegments(Collection<MemorySegment> segments, int size) throws IOException {
		synchronized (factoryLock) {
			numTotalRequiredBuffers -= size;

			availableMemorySegments.addAll(segments);

			// note: if this fails, we're fine for the buffer pool since we already recycled the segments
			redistributeBuffers();
		}
	}

	public void destroy() {
		synchronized (factoryLock) {
			isDestroyed = true;

			MemorySegment segment;
			while ((segment = availableMemorySegments.poll()) != null) {
				segment.free();
			}
		}
	}

	public boolean isDestroyed() {
		return isDestroyed;
	}

	public int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	public int getTotalNumberOfMemorySegments() {
		return totalNumberOfMemorySegments;
	}

	public int getNumberOfAvailableMemorySegments() {
		return availableMemorySegments.size();
	}

	public int getNumberOfRegisteredBufferPools() {
		synchronized (factoryLock) {
			return allBufferPools.size();
		}
	}

	public int countBuffers() {
		int buffers = 0;

		synchronized (factoryLock) {
			for (BufferPool bp : allBufferPools) {
				buffers += bp.getNumBuffers();
			}
		}

		return buffers;
	}

	// ------------------------------------------------------------------------
	// BufferPoolFactory
	// ------------------------------------------------------------------------

	@Override
	public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers) throws IOException {
		return createBufferPool(numRequiredBuffers, maxUsedBuffers, Optional.empty());
	}

	@Override
	public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers, Optional<BufferPoolOwner> owner) throws IOException {
		// It is necessary to use a separate lock from the one used for buffer
		// requests to ensure deadlock freedom for failure cases.
		synchronized (factoryLock) {
			if (isDestroyed) {
				throw new IllegalStateException("Network buffer pool has already been destroyed.");
			}

			// Ensure that the number of required buffers can be satisfied.
			// With dynamic memory management this should become obsolete.
			if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {
				throw new IOException(String.format("Insufficient number of network buffers: " +
								"required %d, but only %d available. The total number of network " +
								"buffers is currently set to %d of %d bytes each. You can increase this " +
								"number by setting the configuration keys '%s', '%s', and '%s'.",
						numRequiredBuffers,
						totalNumberOfMemorySegments - numTotalRequiredBuffers,
						totalNumberOfMemorySegments,
						memorySegmentSize,
						NetworkEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key(),
						NetworkEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.key(),
						NetworkEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key()));
			}

			this.numTotalRequiredBuffers += numRequiredBuffers;

			// We are good to go, create a new buffer pool and redistribute
			// non-fixed size buffers.
			// 因为BufferPool当前只有LocalBufferPool这一个实现，
			// 所以NetworkBufferPool在实现BufferPoolFactory的createBufferPool方法时会直接实例化LocalBufferPool
			LocalBufferPool localBufferPool =
				new LocalBufferPool(this, numRequiredBuffers, maxUsedBuffers, owner);

			allBufferPools.add(localBufferPool);

			try {
				redistributeBuffers();
			} catch (IOException e) {
				try {
					destroyBufferPool(localBufferPool);
				} catch (IOException inner) {
					e.addSuppressed(inner);
				}
				ExceptionUtils.rethrowIOException(e);
			}

			return localBufferPool;
		}
	}

	@Override
	public void destroyBufferPool(BufferPool bufferPool) throws IOException {
		if (!(bufferPool instanceof LocalBufferPool)) {
			throw new IllegalArgumentException("bufferPool is no LocalBufferPool");
		}

		synchronized (factoryLock) {
			if (allBufferPools.remove(bufferPool)) {
				numTotalRequiredBuffers -= bufferPool.getNumberOfRequiredMemorySegments();

				redistributeBuffers();
			}
		}
	}

	/**
	 * Destroys all buffer pools that allocate their buffers from this
	 * buffer pool (created via {@link #createBufferPool(int, int)}).
	 */
	public void destroyAllBufferPools() {
		synchronized (factoryLock) {
			// create a copy to avoid concurrent modification exceptions
			LocalBufferPool[] poolsCopy = allBufferPools.toArray(new LocalBufferPool[allBufferPools.size()]);

			for (LocalBufferPool pool : poolsCopy) {
				pool.lazyDestroy();
			}

			// some sanity checks
			if (allBufferPools.size() > 0 || numTotalRequiredBuffers > 0) {
				throw new IllegalStateException("NetworkBufferPool is not empty after destroying all LocalBufferPools");
			}
		}
	}

	// Must be called from synchronized block
	private void redistributeBuffers() throws IOException {
		assert Thread.holdsLock(factoryLock);

		// All buffers, which are not among the required ones
		final int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;

		if (numAvailableMemorySegment == 0) {
			// in this case, we need to redistribute buffers so that every pool gets its minimum
			for (LocalBufferPool bufferPool : allBufferPools) {
				bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments());
			}
			return;
		}

		/*
		 * With buffer pools being potentially limited, let's distribute the available memory
		 * segments based on the capacity of each buffer pool, i.e. the maximum number of segments
		 * an unlimited buffer pool can take is numAvailableMemorySegment, for limited buffer pools
		 * it may be less. Based on this and the sum of all these values (totalCapacity), we build
		 * a ratio that we use to distribute the buffers.
		 */

		long totalCapacity = 0; // long to avoid int overflow

		for (LocalBufferPool bufferPool : allBufferPools) {
			int excessMax = bufferPool.getMaxNumberOfMemorySegments() -
				bufferPool.getNumberOfRequiredMemorySegments();
			totalCapacity += Math.min(numAvailableMemorySegment, excessMax);
		}

		// no capacity to receive additional buffers?
		if (totalCapacity == 0) {
			return; // necessary to avoid div by zero when nothing to re-distribute
		}

		// since one of the arguments of 'min(a,b)' is a positive int, this is actually
		// guaranteed to be within the 'int' domain
		// (we use a checked downCast to handle possible bugs more gracefully).
		final int memorySegmentsToDistribute = MathUtils.checkedDownCast(
				Math.min(numAvailableMemorySegment, totalCapacity));

		long totalPartsUsed = 0; // of totalCapacity
		int numDistributedMemorySegment = 0;
		for (LocalBufferPool bufferPool : allBufferPools) {
			int excessMax = bufferPool.getMaxNumberOfMemorySegments() -
				bufferPool.getNumberOfRequiredMemorySegments();

			// shortcut
			if (excessMax == 0) {
				continue;
			}

			totalPartsUsed += Math.min(numAvailableMemorySegment, excessMax);

			// avoid remaining buffers by looking at the total capacity that should have been
			// re-distributed up until here
			// the downcast will always succeed, because both arguments of the subtraction are in the 'int' domain
			final int mySize = MathUtils.checkedDownCast(
					memorySegmentsToDistribute * totalPartsUsed / totalCapacity - numDistributedMemorySegment);

			numDistributedMemorySegment += mySize;
			bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments() + mySize);
		}

		assert (totalPartsUsed == totalCapacity);
		assert (numDistributedMemorySegment == memorySegmentsToDistribute);
	}
}
