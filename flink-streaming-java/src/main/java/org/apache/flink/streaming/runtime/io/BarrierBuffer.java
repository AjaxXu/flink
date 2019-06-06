/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.decline.AlignmentLimitExceededException;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineException;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineOnCancellationBarrierException;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineSubsumedException;
import org.apache.flink.runtime.checkpoint.decline.InputEndOfStreamException;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The barrier buffer is {@link CheckpointBarrierHandler} that blocks inputs with barriers until
 * all inputs have received the barrier for a given checkpoint.
 *
 * <p>To avoid back-pressuring the input streams (which may cause distributed deadlocks), the
 * BarrierBuffer continues receiving buffers from the blocked channels and stores them internally until
 * the blocks are released.
 */
@Internal
public class BarrierBuffer implements CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);

	/** The gate that the buffer draws its input from. */
	private final InputGate inputGate;

	/** Flags that indicate whether a channel is currently blocked/buffered. */
	private final boolean[] blockedChannels;

	/** The total number of channels that this buffer handles data from. */
	private final int totalNumberOfInputChannels;

	/** To utility to write blocked data to a file channel. */
	private final BufferBlocker bufferBlocker;

	/**
	 * The pending blocked buffer/event sequences. Must be consumed before requesting further data
	 * from the input gate.
	 */
	private final ArrayDeque<BufferOrEventSequence> queuedBuffered;

	/**
	 * The maximum number of bytes that may be buffered before an alignment is broken. -1 means
	 * unlimited.
	 */
	private final long maxBufferedBytes;

	private final String taskName;

	/**
	 * The sequence of buffers/events that has been unblocked and must now be consumed before
	 * requesting further data from the input gate.
	 */
	private BufferOrEventSequence currentBuffered;

	/** Handler that receives the checkpoint notifications. */
	private AbstractInvokable toNotifyOnCheckpoint;

	/** The ID of the checkpoint for which we expect barriers. */
	private long currentCheckpointId = -1L;

	/**
	 * The number of received barriers (= number of blocked/buffered channels) IMPORTANT: A canceled
	 * checkpoint must always have 0 barriers.
	 */
	private int numBarriersReceived;

	/** The number of already closed channels. */
	private int numClosedChannels;

	/** The number of bytes in the queued spilled sequences. */
	private long numQueuedBytes;

	/** The timestamp as in {@link System#nanoTime()} at which the last alignment started. */
	private long startOfAlignmentTimestamp;

	/** The time (in nanoseconds) that the latest alignment took. */
	private long latestAlignmentDurationNanos;

	/** Flag to indicate whether we have drawn all available input. */
	private boolean endOfStream;

	/** Indicate end of the input. Set to true after encountering {@link #endOfStream} and depleting
	 * {@link #currentBuffered}. */
	private boolean isFinished;

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>There is no limit to how much data may be buffered during an alignment.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferBlocker The buffer blocker to hold the buffers and events for channels with barrier.
	 */
	@VisibleForTesting
	BarrierBuffer(InputGate inputGate, BufferBlocker bufferBlocker) {
		this (inputGate, bufferBlocker, -1, "Testing: No task associated");
	}

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>The aligner will allow only alignments that buffer up to the given number of bytes.
	 * When that number is exceeded, it will stop the alignment and notify the task that the
	 * checkpoint has been cancelled.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferBlocker The buffer blocker to hold the buffers and events for channels with barrier.
	 * @param maxBufferedBytes The maximum bytes to be buffered before the checkpoint aborts.
	 * @param taskName The task name for logging.
	 */
	BarrierBuffer(InputGate inputGate, BufferBlocker bufferBlocker, long maxBufferedBytes, String taskName) {
		checkArgument(maxBufferedBytes == -1 || maxBufferedBytes > 0);

		this.inputGate = inputGate;
		this.maxBufferedBytes = maxBufferedBytes;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.blockedChannels = new boolean[this.totalNumberOfInputChannels];

		this.bufferBlocker = checkNotNull(bufferBlocker);
		this.queuedBuffered = new ArrayDeque<BufferOrEventSequence>();

		this.taskName = taskName;
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		if (currentBuffered == null) {
			return inputGate.isAvailable();
		}
		return AVAILABLE;
	}

	// ------------------------------------------------------------------------
	//  Buffer and barrier handling
	// ------------------------------------------------------------------------

	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		while (true) {
			// process buffered BufferOrEvents before grabbing new ones
			// 获得下一个待缓存的buffer或者barrier事件
			Optional<BufferOrEvent> next;
			//如果当前的缓冲区为null，则从输入端获得
			if (currentBuffered == null) {
				next = inputGate.pollNext();
			}
			//如果缓冲区不为空，则从缓冲区中获得数据
			else {
				// TODO: FLINK-12536 for non credit-based flow control, getNext method is blocking
				next = Optional.ofNullable(currentBuffered.getNext());
				//如果获得的数据为null，则表示缓冲区中已经没有更多地数据了
				if (!next.isPresent()) {
					//清空当前缓冲区，获取新的缓冲区并打开它
					completeBufferedSequence();
					//递归调用，处理下一条数据
					return pollNext();
				}
			}

			//next 为null 同时流结束标识为false
			if (!next.isPresent()) {
				return handleEmptyBuffer();
			}

			//获取到一条记录，不为null
			BufferOrEvent bufferOrEvent = next.get();
			if (isBlocked(bufferOrEvent.getChannelIndex())) {
				// if the channel is blocked, we just store the BufferOrEvent
				//如果获取到得记录所在的channel已经处于阻塞状态，则该记录会被加入缓冲区
				bufferBlocker.add(bufferOrEvent);
				checkSizeLimit();
			}
			//如果该记录是一个正常的记录，而不是一个barrier(事件)，则直接返回
			else if (bufferOrEvent.isBuffer()) {
				return next;
			}
			//如果是一个barrier
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				if (!endOfStream) {
					// process barriers only if there is a chance of the checkpoint completing
					//并且当前流还未处于结束状态，则处理该barrier
					processBarrier((CheckpointBarrier) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
				}
			}
			// 如果是取消Checkpoint事件
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent());
			}
			else {
				//如果它是一个事件，表示当前已到达分区末尾
				if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
					processEndOfPartition();
				}
				//返回该事件
				return next;
			}
		}
	}

	private Optional<BufferOrEvent> handleEmptyBuffer() throws Exception {
		if (!inputGate.isFinished()) {
			return Optional.empty();
		}

		if (endOfStream) {
			isFinished = true;
			return Optional.empty();
		} else {
			// end of input stream. stream continues with the buffered data
			endOfStream = true;
			releaseBlocksAndResetBarriers();
			return pollNext();
		}
	}

	private void completeBufferedSequence() throws IOException {
		LOG.debug("{}: Finished feeding back buffered data.", taskName);

		currentBuffered.cleanup();
		currentBuffered = queuedBuffered.pollFirst();
		if (currentBuffered != null) {
			currentBuffered.open();
			numQueuedBytes -= currentBuffered.size();
		}
	}

	private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
		//获取接收到得barrier的ID
		final long barrierId = receivedBarrier.getId();

		// fast path for single channel cases
		if (totalNumberOfInputChannels == 1) {
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				currentCheckpointId = barrierId;
				notifyCheckpoint(receivedBarrier);
			}
			return;
		}

		// -- general code path for multiple input channels --
		//接收到的barrier数目 > 0 ，说明当前正在处理某个检查点的过程中

		if (numBarriersReceived > 0) {
			// this is only true if some alignment is already progress and was not canceled
			//当前检查点的某个后续的barrierId
			if (barrierId == currentCheckpointId) {
				// regular case
				//处理barrier
				onBarrier(channelIndex);
			}
			//barrierId > 当前检查点Id
			else if (barrierId > currentCheckpointId) {
				// we did not complete the current checkpoint, another started before
				LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					taskName,
					barrierId,
					currentCheckpointId);

				// let the task know we are not completing this
				notifyAbort(currentCheckpointId, new CheckpointDeclineSubsumedException(barrierId));

				// abort the current checkpoint
				releaseBlocksAndResetBarriers();

				// begin a the new checkpoint
				// 开始新的Checkpoint
				beginNewAlignment(barrierId, channelIndex);
			}
			else {
				// ignore trailing barrier from an earlier checkpoint (obsolete now)
				//忽略终止的检查点的barrier，barrierId < currentCheckpointId
				return;
			}
		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint
			//说明这是一个新检查点的初始barrier
			beginNewAlignment(barrierId, channelIndex);
		}
		else {
			// either the current checkpoint was canceled (numBarriers == 0) or
			// this barrier is from an old subsumed checkpoint
			return;
		}

		// check if we have all barriers - since canceled checkpoints always have zero barriers
		// this can only happen on a non canceled checkpoint
		//接收到barriers的数目 + 关闭的channel的数目 = 输入channel的总数目
		if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
			// actually trigger checkpoint
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received all barriers, triggering checkpoint {} at {}.",
					taskName,
					receivedBarrier.getId(),
					receivedBarrier.getTimestamp());
			}

			//解除阻塞
			releaseBlocksAndResetBarriers();
			notifyCheckpoint(receivedBarrier);
		}
	}

	private void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		final long barrierId = cancelBarrier.getCheckpointId();

		// fast path for single channel cases
		if (totalNumberOfInputChannels == 1) {
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				currentCheckpointId = barrierId;
				notifyAbortOnCancellationBarrier(barrierId);
			}
			return;
		}

		// -- general code path for multiple input channels --

		if (numBarriersReceived > 0) {
			// this is only true if some alignment is in progress and nothing was canceled

			if (barrierId == currentCheckpointId) {
				// cancel this alignment
				if (LOG.isDebugEnabled()) {
					LOG.debug("{}: Checkpoint {} canceled, aborting alignment.", taskName, barrierId);
				}

				releaseBlocksAndResetBarriers();
				notifyAbortOnCancellationBarrier(barrierId);
			}
			else if (barrierId > currentCheckpointId) {
				// we canceled the next which also cancels the current
				LOG.warn("{}: Received cancellation barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					taskName,
					barrierId,
					currentCheckpointId);

				// this stops the current alignment
				releaseBlocksAndResetBarriers();

				// the next checkpoint starts as canceled
				currentCheckpointId = barrierId;
				startOfAlignmentTimestamp = 0L;
				latestAlignmentDurationNanos = 0L;

				notifyAbort(currentCheckpointId, new CheckpointDeclineSubsumedException(barrierId));

				notifyAbortOnCancellationBarrier(barrierId);
			}

			// else: ignore trailing (cancellation) barrier from an earlier checkpoint (obsolete now)

		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint is directly a cancellation

			// by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
			// at zero means that no checkpoint barrier can start a new alignment
			currentCheckpointId = barrierId;

			startOfAlignmentTimestamp = 0L;
			latestAlignmentDurationNanos = 0L;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Checkpoint {} canceled, skipping alignment.", taskName, barrierId);
			}

			notifyAbortOnCancellationBarrier(barrierId);
		}

		// else: trailing barrier from either
		//   - a previous (subsumed) checkpoint
		//   - the current checkpoint if it was already canceled
	}

	private void processEndOfPartition() throws Exception {
		//以关闭的channel计数器加一
		numClosedChannels++;

		if (numBarriersReceived > 0) {
			// let the task know we skip a checkpoint
			// 通知task，将跳过这次checkpoint
			notifyAbort(currentCheckpointId, new InputEndOfStreamException());

			// no chance to complete this checkpoint
			//此时已经没有机会完成该检查点，则解除阻塞
			releaseBlocksAndResetBarriers();
		}
	}

	private void notifyCheckpoint(CheckpointBarrier checkpointBarrier) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			CheckpointMetaData checkpointMetaData =
					new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());

			long bytesBuffered = currentBuffered != null ? currentBuffered.size() : 0L;

			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
					.setBytesBufferedInAlignment(bytesBuffered)
					.setAlignmentDurationNanos(latestAlignmentDurationNanos);

			toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
				checkpointMetaData,
				checkpointBarrier.getCheckpointOptions(),
				checkpointMetrics);
		}
	}

	private void notifyAbortOnCancellationBarrier(long checkpointId) throws Exception {
		notifyAbort(checkpointId, new CheckpointDeclineOnCancellationBarrierException());
	}

	private void notifyAbort(long checkpointId, CheckpointDeclineException cause) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
		}
	}

	private void checkSizeLimit() throws Exception {
		if (maxBufferedBytes > 0 && (numQueuedBytes + bufferBlocker.getBytesBlocked()) > maxBufferedBytes) {
			// exceeded our limit - abort this checkpoint
			LOG.info("{}: Checkpoint {} aborted because alignment volume limit ({} bytes) exceeded.",
				taskName,
				currentCheckpointId,
				maxBufferedBytes);

			releaseBlocksAndResetBarriers();
			notifyAbort(currentCheckpointId, new AlignmentLimitExceededException(maxBufferedBytes));
		}
	}

	@Override
	public void registerCheckpointEventHandler(AbstractInvokable toNotifyOnCheckpoint) {
		if (this.toNotifyOnCheckpoint == null) {
			this.toNotifyOnCheckpoint = toNotifyOnCheckpoint;
		}
		else {
			throw new IllegalStateException("BarrierBuffer already has a registered checkpoint notifyee");
		}
	}

	@Override
	public boolean isEmpty() {
		return currentBuffered == null;
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public void cleanup() throws IOException {
		bufferBlocker.close();
		if (currentBuffered != null) {
			currentBuffered.cleanup();
		}
		for (BufferOrEventSequence seq : queuedBuffered) {
			seq.cleanup();
		}
		queuedBuffered.clear();
		numQueuedBytes = 0L;
	}

	private void beginNewAlignment(long checkpointId, int channelIndex) throws IOException {
		currentCheckpointId = checkpointId;
		onBarrier(channelIndex);

		startOfAlignmentTimestamp = System.nanoTime();

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Starting stream alignment for checkpoint {}.", taskName, checkpointId);
		}
	}

	/**
	 * Checks whether the channel with the given index is blocked.
	 *
	 * @param channelIndex The channel index to check.
	 * @return True if the channel is blocked, false if not.
	 */
	private boolean isBlocked(int channelIndex) {
		return blockedChannels[channelIndex];
	}

	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 * 将barrier关联的channel标识为阻塞状态同时将barrier计数器加一
	 * @param channelIndex The channel index to block.
	 */
	private void onBarrier(int channelIndex) throws IOException {
		if (!blockedChannels[channelIndex]) {
			blockedChannels[channelIndex] = true;

			numBarriersReceived++;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received barrier from channel {}.", taskName, channelIndex);
			}
		}
		else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint on input " + channelIndex);
		}
	}

	/**
	 * Releases the blocks on all channels and resets the barrier count.
	 * Makes sure the just written data is the next to be consumed.
	 */
	private void releaseBlocksAndResetBarriers() throws IOException {
		LOG.debug("{}: End of stream alignment, feeding buffered data back.", taskName);

		//将所有channel的阻塞标识置为false
		for (int i = 0; i < blockedChannels.length; i++) {
			blockedChannels[i] = false;
		}

		//如果当前的缓冲区中的数据为空
		if (currentBuffered == null) {
			// common case: no more buffered data
			//初始化新的缓冲区读写器
			currentBuffered = bufferBlocker.rollOverReusingResources();
			//打开缓冲区读写器
			if (currentBuffered != null) {
				currentBuffered.open();
			}
		}
		else {
			// uncommon case: buffered data pending
			// push back the pending data, if we have any
			LOG.debug("{}: Checkpoint skipped via buffered data:" +
					"Pushing back current alignment buffers and feeding back new alignment data first.", taskName);

			// since we did not fully drain the previous sequence, we need to allocate a new buffer for this one
			//缓冲区中还有数据，则初始化一块新的存储空间来存储新的缓冲数据
			BufferOrEventSequence bufferedNow = bufferBlocker.rollOverWithoutReusingResources();
			if (bufferedNow != null) {
				//打开新的缓冲区读写器
				bufferedNow.open();
				//将当前没有处理完的数据加入队列中
				queuedBuffered.addFirst(currentBuffered);
				numQueuedBytes += currentBuffered.size();
				//将新开辟的缓冲区读写器置为新的当前缓冲区
				currentBuffered = bufferedNow;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Size of buffered data: {} bytes",
				taskName,
				currentBuffered == null ? 0L : currentBuffered.size());
		}

		// the next barrier that comes must assume it is the first
		//将接收到的barrier累加值重置为0
		numBarriersReceived = 0;

		if (startOfAlignmentTimestamp > 0) {
			latestAlignmentDurationNanos = System.nanoTime() - startOfAlignmentTimestamp;
			startOfAlignmentTimestamp = 0;
		}
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 *
	 * @return The ID of the pending of completed checkpoint.
	 */
	public long getCurrentCheckpointId() {
		return this.currentCheckpointId;
	}

	@Override
	public long getAlignmentDurationNanos() {
		long start = this.startOfAlignmentTimestamp;
		if (start <= 0) {
			return latestAlignmentDurationNanos;
		} else {
			return System.nanoTime() - start;
		}
	}

	@Override
	public int getNumberOfInputChannels() {
		return totalNumberOfInputChannels;
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("%s: last checkpoint: %d, current barriers: %d, closed channels: %d",
			taskName,
			currentCheckpointId,
			numBarriersReceived,
			numClosedChannels);
	}
}
