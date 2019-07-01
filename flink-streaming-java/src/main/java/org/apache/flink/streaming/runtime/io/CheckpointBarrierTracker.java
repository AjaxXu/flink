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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;

/**
 * BarrierTracker会对各个input channel接收到的检查点的barrier进行跟踪。
 * 一旦它观察到某个检查点的所有barrier都已经到达，它将会通知监听器检查点已完成，以触发相应地回调处理
 * The {@link CheckpointBarrierTracker} keeps track of what checkpoint barriers have been received from
 * which input channels. Once it has observed all checkpoint barriers for a checkpoint ID,
 * it notifies its listener of a completed checkpoint.
 * 不像BarrierBuffer，BarrierTracker不阻塞已经发送了barrier的input channel，所以它不能提供exactly-once的一致性保证。
 *  但是它可以提供at least once的一致性保证。
 * <p>Unlike the {@link CheckpointBarrierAligner}, the BarrierTracker does not block the input
 * channels that have sent barriers, so it cannot be used to gain "exactly-once" processing
 * guarantees. It can, however, be used to gain "at least once" processing guarantees.
 *
 * <p>NOTE: This implementation strictly assumes that newer checkpoints have higher checkpoint IDs.
 *
 * 这里不阻塞input channel，也就说明不采用对齐机制，因此本检查点的数据会及时被处理，
 * 并且因此下一个检查点的数据可能会在该检查点还没有完成时就已经到来。所以，在恢复时只能提供AT_LEAST_ONCE保证
 */
@Internal
public class CheckpointBarrierTracker extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBarrierTracker.class);

	/**
	 * The tracker tracks a maximum number of checkpoints, for which some, but not all barriers
	 * have yet arrived.
	 */
	private static final int MAX_CHECKPOINTS_TO_TRACK = 50;

	// ------------------------------------------------------------------------

	/**
	 * The number of channels. Once that many barriers have been received for a checkpoint, the
	 * checkpoint is considered complete.
	 */
	private final int totalNumberOfInputChannels;

	/**
	 * All checkpoints for which some (but not all) barriers have been received, and that are not
	 * yet known to be subsumed by newer checkpoints.
	 */
	private final ArrayDeque<CheckpointBarrierCount> pendingCheckpoints;

	/** The highest checkpoint ID encountered so far. */
	private long latestPendingCheckpointID = -1;

	public CheckpointBarrierTracker(int totalNumberOfInputChannels) {
		this(totalNumberOfInputChannels, null);
	}

	public CheckpointBarrierTracker(int totalNumberOfInputChannels, @Nullable AbstractInvokable toNotifyOnCheckpoint) {
		super(toNotifyOnCheckpoint);
		this.totalNumberOfInputChannels = totalNumberOfInputChannels;
		this.pendingCheckpoints = new ArrayDeque<>();
	}

	@Override
	public void releaseBlocksAndResetBarriers() {
	}

	@Override
	public boolean isBlocked(int channelIndex) {
		return false;
	}

	@Override
	public boolean processBarrier(CheckpointBarrier receivedBarrier, int channelIndex, long bufferedBytes) throws Exception {
		final long barrierId = receivedBarrier.getId();

		// fast path for single channel trackers
		//首先判断特殊情况：当前operator是否只有一个input channel
		//如果是，那么就省略了统计的步骤，直接触发barrier handler回调
		if (totalNumberOfInputChannels == 1) {
			notifyCheckpoint(receivedBarrier, 0, 0);
			return false;
		}

		// general path for multiple input channels
		//判断通常状态：当前operator存在多个input channel
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received barrier for checkpoint {} from channel {}", barrierId, channelIndex);
		}

		// find the checkpoint barrier in the queue of pending barriers
		//所有未完成的检查点都存储在一个队列里，需要找到当前barrier对应的检查点
		CheckpointBarrierCount barrierCount = null;
		int pos = 0;

		for (CheckpointBarrierCount next : pendingCheckpoints) {
			if (next.checkpointId == barrierId) {
				//如果找到则跳出循环
				barrierCount = next;
				break;
			}
			//没找到位置加一
			pos++;
		}

		//最终找到了对应的未完成的检查点
		if (barrierCount != null) {
			// add one to the count to that barrier and check for completion
			//将barrier计数器加一
			int numBarriersNew = barrierCount.incrementBarrierCount();
			//如果barrier计数器等于input channel的总数
			if (numBarriersNew == totalNumberOfInputChannels) {
				// checkpoint can be triggered (or is aborted and all barriers have been seen)
				// first, remove this checkpoint and all all prior pending
				// checkpoints (which are now subsumed)
				//移除pos之前的所有检查点（检查点在队列中得先后顺序跟检查点的时序是一致的）
				for (int i = 0; i <= pos; i++) {
					pendingCheckpoints.pollFirst();
				}

				// notify the listener
				if (!barrierCount.isAborted()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Received all barriers for checkpoint {}", barrierId);
					}

					//触发检查点处理器事件
					notifyCheckpoint(receivedBarrier, 0, 0);
				}
			}
		}
		//如果没有找到对应的检查点，则说明该barrier有可能是新检查点的第一个barrier
		else {
			// first barrier for that checkpoint ID
			// add it only if it is newer than the latest checkpoint.
			// if it is not newer than the latest checkpoint ID, then there cannot be a
			// successful checkpoint for that ID anyways
			//如果是比当前最新的检查点编号还大，则说明是新检查点
			if (barrierId > latestPendingCheckpointID) {
				latestPendingCheckpointID = barrierId;
				pendingCheckpoints.addLast(new CheckpointBarrierCount(barrierId));

				// make sure we do not track too many checkpoints
				//如果超出阈值，则移除最老的检查点
				if (pendingCheckpoints.size() > MAX_CHECKPOINTS_TO_TRACK) {
					pendingCheckpoints.pollFirst();
				}
			}
		}
		return false;
	}

	@Override
	public boolean processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		final long checkpointId = cancelBarrier.getCheckpointId();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Received cancellation barrier for checkpoint {}", checkpointId);
		}

		// fast path for single channel trackers
		if (totalNumberOfInputChannels == 1) {
			notifyAbortOnCancellationBarrier(checkpointId);
			return false;
		}

		// -- general path for multiple input channels --

		// find the checkpoint barrier in the queue of pending barriers
		// while doing this we "abort" all checkpoints before that one
		CheckpointBarrierCount cbc;
		while ((cbc = pendingCheckpoints.peekFirst()) != null && cbc.checkpointId() < checkpointId) {
			pendingCheckpoints.removeFirst();

			if (cbc.markAborted()) {
				// abort the subsumed checkpoints if not already done
				notifyAbortOnCancellationBarrier(cbc.checkpointId());
			}
		}

		if (cbc != null && cbc.checkpointId() == checkpointId) {
			// make sure the checkpoint is remembered as aborted
			if (cbc.markAborted()) {
				// this was the first time the checkpoint was aborted - notify
				notifyAbortOnCancellationBarrier(checkpointId);
			}

			// we still count the barriers to be able to remove the entry once all barriers have been seen
			if (cbc.incrementBarrierCount() == totalNumberOfInputChannels) {
				// we can remove this entry
				pendingCheckpoints.removeFirst();
			}
		}
		else if (checkpointId > latestPendingCheckpointID) {
			notifyAbortOnCancellationBarrier(checkpointId);

			latestPendingCheckpointID = checkpointId;

			CheckpointBarrierCount abortedMarker = new CheckpointBarrierCount(checkpointId);
			abortedMarker.markAborted();
			pendingCheckpoints.addFirst(abortedMarker);

			// we have removed all other pending checkpoint barrier counts --> no need to check that
			// we don't exceed the maximum checkpoints to track
		} else {
			// trailing cancellation barrier which was already cancelled
		}
		return false;
	}

	@Override
	public boolean processEndOfPartition() throws Exception {
		while (!pendingCheckpoints.isEmpty()) {
			CheckpointBarrierCount barrierCount = pendingCheckpoints.removeFirst();
			if (barrierCount.markAborted()) {
				notifyAbort(barrierCount.checkpointId(),
					new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));
			}
		}
		return false;
	}

	public long getLatestCheckpointId() {
		return pendingCheckpoints.isEmpty() ? -1 : pendingCheckpoints.peekLast().checkpointId();
	}

	public long getAlignmentDurationNanos() {
		return 0;
	}

	@Override
	public void checkpointSizeLimitExceeded(long maxBufferedBytes) throws Exception {
		throw new UnsupportedOperationException("This should never happened as this class doesn't block any data");
	}

	/**
	 * Simple class for a checkpoint ID with a barrier counter.
	 */
	private static final class CheckpointBarrierCount {

		private final long checkpointId;

		private int barrierCount;

		private boolean aborted;

		CheckpointBarrierCount(long checkpointId) {
			this.checkpointId = checkpointId;
			this.barrierCount = 1;
		}

		public long checkpointId() {
			return checkpointId;
		}

		public int incrementBarrierCount() {
			return ++barrierCount;
		}

		public boolean isAborted() {
			return aborted;
		}

		public boolean markAborted() {
			boolean firstAbort = !this.aborted;
			this.aborted = true;
			return firstAbort;
		}

		@Override
		public String toString() {
			return isAborted() ?
				String.format("checkpointID=%d - ABORTED", checkpointId) :
				String.format("checkpointID=%d, count=%d", checkpointId, barrierCount);
		}
	}
}
