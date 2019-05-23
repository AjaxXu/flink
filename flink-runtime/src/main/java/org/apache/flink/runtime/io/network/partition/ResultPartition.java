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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A result partition for data produced by a single task.
 *
 * <p>This class is the runtime part of a logical {@link IntermediateResultPartition}. Essentially,
 * a result partition is a collection of {@link Buffer} instances. The buffers are organized in one
 * or more {@link ResultSubpartition} instances, which further partition the data depending on the
 * number of consuming tasks and the data {@link DistributionPattern}.
 *
 * <p>Tasks, which consume a result partition have to request one of its subpartitions. The request
 * happens either remotely (see {@link RemoteInputChannel}) or locally (see {@link LocalInputChannel})
 *
 * <h2>Life-cycle</h2>
 *
 * <p>The life-cycle of each result partition has three (possibly overlapping) phases:
 * <ol>
 * <li><strong>Produce</strong>: </li>
 * <li><strong>Consume</strong>: </li>
 * <li><strong>Release</strong>: </li>
 * </ol>
 *
 * <h2>Lazy deployment and updates of consuming tasks</h2>
 *
 * <p>Before a consuming task can request the result, it has to be deployed. The time of deployment
 * depends on the PIPELINED vs. BLOCKING characteristic of the result partition. With pipelined
 * results, receivers are deployed as soon as the first buffer is added to the result partition.
 * With blocking results on the other hand, receivers are deployed after the partition is finished.
 *
 * <h2>Buffer management</h2>
 *
 * <h2>State management</h2>
 */
public class ResultPartition implements ResultPartitionWriter, BufferPoolOwner {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartition.class);

	private final String owningTaskName;

	private final TaskActions taskActions;

	private final JobID jobId;

	// 结果分区编号（ResultPartitionID）用来标识ResultPartition
	private final ResultPartitionID partitionId;

	/** Type of this partition. Defines the concrete subpartition implementation to use. */
	private final ResultPartitionType partitionType;

	/** The subpartitions of this partition. At least one. */
	private final ResultSubpartition[] subpartitions;

	private final ResultPartitionManager partitionManager;

	private final ResultPartitionConsumableNotifier partitionConsumableNotifier;

	public final int numTargetKeyGroups;

	private final boolean sendScheduleOrUpdateConsumersMessage;

	// - Runtime state --------------------------------------------------------

	private final AtomicBoolean isReleased = new AtomicBoolean();

	/**
	 * The total number of references to subpartitions of this result. The result partition can be
	 * safely released, iff the reference count is zero. A reference count of -1 denotes that the
	 * result partition has been released.
	 */
	private final AtomicInteger pendingReferences = new AtomicInteger();

	private BufferPool bufferPool;

	// 用来表示当前是否已通知过消费者
	private boolean hasNotifiedPipelinedConsumers;

	private boolean isFinished;

	private volatile Throwable cause;

	private final FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory;

	public ResultPartition(
		String owningTaskName,
		TaskActions taskActions, // actions on the owning task
		JobID jobId,
		ResultPartitionID partitionId,
		ResultPartitionType partitionType,
		ResultSubpartition[] subpartitions,
		int numTargetKeyGroups,
		ResultPartitionManager partitionManager,
		ResultPartitionConsumableNotifier partitionConsumableNotifier,
		boolean sendScheduleOrUpdateConsumersMessage,
		FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {

		this.owningTaskName = checkNotNull(owningTaskName);
		this.taskActions = checkNotNull(taskActions);
		this.jobId = checkNotNull(jobId);
		this.partitionId = checkNotNull(partitionId);
		this.partitionType = checkNotNull(partitionType);
		this.subpartitions = checkNotNull(subpartitions);
		this.numTargetKeyGroups = numTargetKeyGroups;
		this.partitionManager = checkNotNull(partitionManager);
		this.partitionConsumableNotifier = checkNotNull(partitionConsumableNotifier);
		this.sendScheduleOrUpdateConsumersMessage = sendScheduleOrUpdateConsumersMessage;
		this.bufferPoolFactory = bufferPoolFactory;
	}

	/**
	 * Registers a buffer pool with this result partition.
	 *
	 * <p>There is one pool for each result partition, which is shared by all its sub partitions.
	 *
	 * <p>The pool is registered with the partition *after* it as been constructed in order to conform
	 * to the life-cycle of task registrations in the {@link TaskExecutor}.
	 */
	@Override
	public void setup() throws IOException {
		checkState(this.bufferPool == null, "Bug in result partition setup logic: Already registered buffer pool.");

		BufferPool bufferPool = checkNotNull(bufferPoolFactory.apply(this));
		checkArgument(bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
			"Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for this result partition.");

		this.bufferPool = bufferPool;
		partitionManager.registerResultPartition(this);
	}

	public JobID getJobId() {
		return jobId;
	}

	public String getOwningTaskName() {
		return owningTaskName;
	}

	public ResultPartitionID getPartitionId() {
		return partitionId;
	}

	@Override
	public int getNumberOfSubpartitions() {
		return subpartitions.length;
	}

	@Override
	public BufferProvider getBufferProvider() {
		return bufferPool;
	}

	public BufferPool getBufferPool() {
		return bufferPool;
	}

	public int getNumberOfQueuedBuffers() {
		int totalBuffers = 0;

		for (ResultSubpartition subpartition : subpartitions) {
			totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
		}

		return totalBuffers;
	}

	/**
	 * Returns the type of this result partition.
	 *
	 * @return result partition type
	 */
	public ResultPartitionType getPartitionType() {
		return partitionType;
	}

	// ------------------------------------------------------------------------

	@Override
	public void addBufferConsumer(BufferConsumer bufferConsumer, int subpartitionIndex) throws IOException {
		checkNotNull(bufferConsumer);

		ResultSubpartition subpartition;
		try {
			//确认生产状态处于未完成状态
			checkInProduceState();
			//获取指定索引的子分区
			subpartition = subpartitions[subpartitionIndex];
		}
		catch (Exception ex) {
			bufferConsumer.close();
			throw ex;
		}

		if (subpartition.add(bufferConsumer)) {
			//如果BufferConsumer被加入成功，且当前的模式是管道模式，则立即通知消费者任务
			notifyPipelinedConsumers();
		}
	}

	@Override
	public void flushAll() {
		for (ResultSubpartition subpartition : subpartitions) {
			subpartition.flush();
		}
	}

	@Override
	public void flush(int subpartitionIndex) {
		subpartitions[subpartitionIndex].flush();
	}

	/**
	 * Finishes the result partition.
	 *
	 * <p>After this operation, it is not possible to add further data to the result partition.
	 *
	 * <p>For BLOCKING results, this will trigger the deployment of consuming tasks.
	 * 针对阻塞模式的ResultPartition的通知时机却需要等到数据生产完成之后（ResultPartition的finish方法被调用），
	 * 通知消费者开始消费
	 */
	public void finish() throws IOException {
		boolean success = false;

		try {
			checkInProduceState();

			for (ResultSubpartition subpartition : subpartitions) {
				subpartition.finish();
			}

			success = true;
		}
		finally {
			if (success) {
				isFinished = true;

				notifyPipelinedConsumers();
			}
		}
	}

	public void release() {
		release(null);
	}

	/**
	 * Releases the result partition.
	 */
	public void release(Throwable cause) {
		if (isReleased.compareAndSet(false, true)) {
			LOG.debug("{}: Releasing {}.", owningTaskName, this);

			// Set the error cause
			if (cause != null) {
				this.cause = cause;
			}

			// Release all subpartitions
			for (ResultSubpartition subpartition : subpartitions) {
				try {
					subpartition.release();
				}
				// Catch this in order to ensure that release is called on all subpartitions
				catch (Throwable t) {
					LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
				}
			}
		}
	}

	@Override
	public void close() {
		if (bufferPool != null) {
			bufferPool.lazyDestroy();
		}
	}

	public void fail(@Nullable Throwable throwable) {
		partitionManager.releasePartition(partitionId, throwable);
	}

	/**
	 * Returns the requested subpartition.
	 */
	public ResultSubpartitionView createSubpartitionView(int index, BufferAvailabilityListener availabilityListener) throws IOException {
		int refCnt = pendingReferences.get();

		checkState(refCnt != -1, "Partition released.");
		checkState(refCnt > 0, "Partition not pinned.");

		checkElementIndex(index, subpartitions.length, "Subpartition not found.");

		ResultSubpartitionView readView = subpartitions[index].createReadView(availabilityListener);

		LOG.debug("Created {}", readView);

		return readView;
	}

	public Throwable getFailureCause() {
		return cause;
	}

	@Override
	public int getNumTargetKeyGroups() {
		return numTargetKeyGroups;
	}

	/**
	 * Releases buffers held by this result partition.
	 *
	 * <p>This is a callback from the buffer pool, which is registered for result partitions, which
	 * are back pressure-free.
	 */
	@Override
	public void releaseMemory(int toRelease) throws IOException {
		checkArgument(toRelease > 0);

		for (ResultSubpartition subpartition : subpartitions) {
			toRelease -= subpartition.releaseMemory(); // //让subpartition去releaseMemory

			// Only release as much memory as needed
			if (toRelease <= 0) {
				break;
			}
		}
	}

	/**
	 * Whether this partition is released.
	 *
	 * <p>A partition is released when each subpartition is either consumed and communication is closed by consumer
	 * or failed. A partition is also released if task is cancelled.
	 */
	public boolean isReleased() {
		return isReleased.get();
	}

	@Override
	public String toString() {
		return "ResultPartition " + partitionId.toString() + " [" + partitionType + ", "
				+ subpartitions.length + " subpartitions, "
				+ pendingReferences + " pending references]";
	}

	// ------------------------------------------------------------------------

	/**
	 * Pins the result partition.
	 *
	 * <p>The partition can only be released after each subpartition has been consumed once per pin
	 * operation.
	 */
	void pin() {
		while (true) {
			int refCnt = pendingReferences.get();

			if (refCnt >= 0) {
				if (pendingReferences.compareAndSet(refCnt, refCnt + subpartitions.length)) {
					break;
				}
			}
			else {
				throw new IllegalStateException("Released.");
			}
		}
	}

	/**
	 * Notification when a subpartition is released.
	 */
	void onConsumedSubpartition(int subpartitionIndex) {

		//已被释放，则直接返回
		if (isReleased.get()) {
			return;
		}

		//计数器减一后获得未完成的子分区计数
		int refCnt = pendingReferences.decrementAndGet();

		//如果全部都已完成，则通知ResultPartitionManager，它会将ResultPartition直接释放
		if (refCnt == 0) {
			partitionManager.onConsumedPartition(this);
		}
		//异常
		else if (refCnt < 0) {
			throw new IllegalStateException("All references released.");
		}

		LOG.debug("{}: Received release notification for subpartition {} (reference count now at: {}).",
				this, subpartitionIndex, pendingReferences);
	}

	public ResultSubpartition[] getAllPartitions() {
		return subpartitions;
	}

	// ------------------------------------------------------------------------

	private void checkInProduceState() throws IllegalStateException {
		checkState(!isFinished, "Partition already finished.");
	}

	/**
	 * Notifies pipelined consumers of this result partition once.
	 * 通过分区可消费通知器（ResultPartitionConsumableNotifier）间接通知消费者任务（经过JobManager转发通知）
	 */
	private void notifyPipelinedConsumers() {
		if (sendScheduleOrUpdateConsumersMessage && !hasNotifiedPipelinedConsumers && partitionType.isPipelined()) {
			partitionConsumableNotifier.notifyPartitionConsumable(jobId, partitionId, taskActions);

			// 一旦通知过，该标识将会被设置为true，所以该通知只会发生在第一个被成功加入的BufferConsumer之后，后续便不再通知
			hasNotifiedPipelinedConsumers = true;
		}
	}
}
