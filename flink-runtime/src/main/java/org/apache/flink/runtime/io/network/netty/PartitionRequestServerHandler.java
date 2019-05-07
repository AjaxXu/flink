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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.netty.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.netty.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;

/**
 * PartitionRequestServerHandler是一种通道流入处理器（ChannelInboundHandler），主要用于初始化数据传输同时分发事件
 * Channel handler to initiate data transfers and dispatch backwards flowing task events.
 */
class PartitionRequestServerHandler extends SimpleChannelInboundHandler<NettyMessage> {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

	private final ResultPartitionProvider partitionProvider;

	private final TaskEventPublisher taskEventPublisher;

	private final PartitionRequestQueue outboundQueue; // 发送队列

	private final boolean creditBasedEnabled;

	PartitionRequestServerHandler(
		ResultPartitionProvider partitionProvider,
		TaskEventPublisher taskEventPublisher,
		PartitionRequestQueue outboundQueue,
		boolean creditBasedEnabled) {

		this.partitionProvider = partitionProvider;
		this.taskEventPublisher = taskEventPublisher;
		this.outboundQueue = outboundQueue;
		this.creditBasedEnabled = creditBasedEnabled;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		super.channelRegistered(ctx);
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		super.channelUnregistered(ctx);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, NettyMessage msg) throws Exception {
		try {
			Class<?> msgClazz = msg.getClass();

			// ----------------------------------------------------------------
			// Intermediate result partition requests
			// ----------------------------------------------------------------
			// 消息类型为用户发起的分区请求，即请求发送数据
			if (msgClazz == PartitionRequest.class) {
				PartitionRequest request = (PartitionRequest) msg;

				LOG.debug("Read channel on {}: {}.", ctx.channel().localAddress(), request);

				try {
					NetworkSequenceViewReader reader;
					if (creditBasedEnabled) {
						reader = new CreditBasedSequenceNumberingViewReader(
							request.receiverId,
							request.credit,
							outboundQueue);
					} else {
						reader = new SequenceNumberingViewReader(
							request.receiverId,
							outboundQueue);
					}

					// 创建对应的subPartition视图
					reader.requestSubpartitionView(
						partitionProvider,
						request.partitionId,
						request.queueIndex);

					outboundQueue.notifyReaderCreated(reader);
				} catch (PartitionNotFoundException notFound) {
					respondWithError(ctx, notFound, request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			else if (msgClazz == TaskEventRequest.class) {
				TaskEventRequest request = (TaskEventRequest) msg;

				//针对事件请求，将会通过任务事件分发器进行分发，如果分发失败，将会以错误消息予以响应
				if (!taskEventPublisher.publish(request.partitionId, request.event)) {
					respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
				}
			} else if (msgClazz == CancelPartitionRequest.class) {
				//如果是取消请求，则调用队列的取消方法
				CancelPartitionRequest request = (CancelPartitionRequest) msg;

				outboundQueue.cancel(request.receiverId);
			} else if (msgClazz == CloseRequest.class) {
				//如果是关闭请求，则关闭队列
				outboundQueue.close();
			} else if (msgClazz == AddCredit.class) {
				AddCredit request = (AddCredit) msg;

				outboundQueue.addCredit(request.receiverId, request.credit);
			} else {
				LOG.warn("Received unexpected client request: {}", msg);
			}
		} catch (Throwable t) {
			respondWithError(ctx, t);
		}
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error) {
		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error));
	}

	private void respondWithError(ChannelHandlerContext ctx, Throwable error, InputChannelID sourceId) {
		LOG.debug("Responding with error: {}.", error.getClass());

		ctx.writeAndFlush(new NettyMessage.ErrorResponse(error, sourceId));
	}
}
