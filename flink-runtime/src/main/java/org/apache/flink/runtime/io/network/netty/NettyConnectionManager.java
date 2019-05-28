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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

/**
 * Netty连接管理器（NettyConnectionManager）是连接管理器接口（ConnectionManager）针对基于Netty的远程连接管理的实现者。
 * 它是TaskManager中负责网络通信的网络环境对象（NetworkEnvironment）的核心部件之一。
 *
 * 一个TaskManager中可能同时运行着很多任务实例，有时某些任务需要消费某远程任务所生产的结果分区，有时某些任务可能会生产
 * 结果分区供其他任务消费。所以对一个TaskManager来说，其职责并非单一的，它既可能充当客户端的角色也可能充当服务端角色。
 * 因此，一个NettyConnectionManager会同时管理着一个Netty客户端（NettyClient）和一个Netty服务器（NettyServer）实例。
 * 当然除此之外还有一个Netty缓冲池（NettyBufferPool）以及一个分区请求客户端工厂（PartitionRequestClientFactory，
 * 用于创建分区请求客户端PartitionRequestClient），这些对象都在NettyConnectionManager构造器中被初始化。
 *
 */
import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConnectionManager implements ConnectionManager {

	private final NettyServer server;

	private final NettyClient client;

	private final NettyBufferPool bufferPool;

	// 用于创建分区请求客户端PartitionRequestClient
	private final PartitionRequestClientFactory partitionRequestClientFactory;

	private final NettyProtocol nettyProtocol;

	public NettyConnectionManager(
		ResultPartitionProvider partitionProvider,
		TaskEventPublisher taskEventPublisher,
		NettyConfig nettyConfig,
		boolean isCreditBased) {

		this.server = new NettyServer(nettyConfig);
		this.client = new NettyClient(nettyConfig);
		this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());

		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client);

		this.nettyProtocol = new NettyProtocol(checkNotNull(partitionProvider), checkNotNull(taskEventPublisher), isCreditBased);
	}

	/**
	 * Netty客户端和服务器对象的启动和停止都是由NettyConnectionManager统一控制的
	 * @throws IOException
	 */
	@Override
	public void start() throws IOException {
		client.init(nettyProtocol, bufferPool);
		server.init(nettyProtocol, bufferPool);
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
			throws IOException, InterruptedException {
		return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
	}

	@Override
	public void closeOpenChannelConnections(ConnectionID connectionId) {
		partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
	}

	@Override
	public int getNumberOfActiveConnections() {
		return partitionRequestClientFactory.getNumberOfActiveClients();
	}

	@Override
	public int getDataPort() {
		if (server != null && server.getLocalAddress() != null) {
			return server.getLocalAddress().getPort();
		} else {
			return -1;
		}
	}

	@Override
	public void shutdown() {
		client.shutdown();
		server.shutdown();
	}

	NettyClient getClient() {
		return client;
	}

	NettyServer getServer() {
		return server;
	}

	NettyBufferPool getBufferPool() {
		return bufferPool;
	}
}
