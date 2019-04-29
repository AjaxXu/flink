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

package org.apache.flink.runtime.io.network;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 网络环境（NetworkEnvironment）是TaskManager进行网络通信的主对象，主要用于跟踪中间结果并负责所有的数据交换。
 * 每个TaskManager的实例都包含一个网络环境对象，在TaskManager启动时创建
 * Network I/O components of each {@link TaskExecutor} instance. The network environment contains
 * the data structures that keep track of all intermediate results and all data exchanges.
 */
public class NetworkEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(NetworkEnvironment.class);

	private static final String METRIC_GROUP_NETWORK = "Network";
	private static final String METRIC_TOTAL_MEMORY_SEGMENT = "TotalMemorySegments";
	private static final String METRIC_AVAILABLE_MEMORY_SEGMENT = "AvailableMemorySegments";

	private final Object lock = new Object();

	private final NetworkEnvironmentConfiguration config;

	// 网络缓冲池，负责申请一个TaskManager的所有的内存段用作缓冲池；
	private final NetworkBufferPool networkBufferPool;

	// 连接管理器，用于管理本地（远程）通信连接
	private final ConnectionManager connectionManager;

	// 结果分区管理器，用于跟踪一个TaskManager上所有生产/消费相关的ResultPartition
	private final ResultPartitionManager resultPartitionManager;

	// 任务事件分发器，从消费者任务分发事件给生产者任务；
	private final TaskEventPublisher taskEventPublisher;

	private boolean isShutdown;

	public NetworkEnvironment(
			NetworkEnvironmentConfiguration config,
			TaskEventPublisher taskEventPublisher,
			MetricGroup metricGroup) {
		this.config = checkNotNull(config);

		// 首先根据配置创建网络缓冲池（NetworkBufferPool）
		this.networkBufferPool = new NetworkBufferPool(config.numNetworkBuffers(), config.networkBufferSize());

		NettyConfig nettyConfig = config.nettyConfig();
		if (nettyConfig != null) {
			this.connectionManager = new NettyConnectionManager(nettyConfig, config.isCreditBased());
		} else {
			this.connectionManager = new LocalConnectionManager();
		}

		this.resultPartitionManager = new ResultPartitionManager();

		this.taskEventPublisher = checkNotNull(taskEventPublisher);

		registerNetworkMetrics(metricGroup, networkBufferPool);

		isShutdown = false;
	}

	private static void registerNetworkMetrics(MetricGroup metricGroup, NetworkBufferPool networkBufferPool) {
		checkNotNull(metricGroup);

		MetricGroup networkGroup = metricGroup.addGroup(METRIC_GROUP_NETWORK);
		networkGroup.<Integer, Gauge<Integer>>gauge(METRIC_TOTAL_MEMORY_SEGMENT,
			networkBufferPool::getTotalNumberOfMemorySegments);
		networkGroup.<Integer, Gauge<Integer>>gauge(METRIC_AVAILABLE_MEMORY_SEGMENT,
			networkBufferPool::getNumberOfAvailableMemorySegments);
	}

	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------

	public ResultPartitionManager getResultPartitionManager() {
		return resultPartitionManager;
	}

	public ConnectionManager getConnectionManager() {
		return connectionManager;
	}

	@VisibleForTesting
	public NetworkBufferPool getNetworkBufferPool() {
		return networkBufferPool;
	}

	public NetworkEnvironmentConfiguration getConfiguration() {
		return config;
	}

	// --------------------------------------------------------------------------------------------
	//  Task operations
	//  NetworkEnvironment对象会为当前任务生产端的每个ResultPartition都创建本地缓冲池，缓冲池中的Buffer数为结果分区的子分区数，
	//  同时为当前任务消费端的InputGate创建本地缓冲池，缓冲池的Buffer数为InputGate所包含的输入信道数。
	//  这些缓冲池都是非固定大小的，也就是说他们会按照网络缓冲池内存段的使用情况进行重平衡。
	// --------------------------------------------------------------------------------------------

	public void registerTask(Task task) throws IOException {
		//获得当前任务对象所生产的结果分区集合
		final ResultPartition[] producedPartitions = task.getProducedPartitions();

		synchronized (lock) {
			if (isShutdown) {
				throw new IllegalStateException("NetworkEnvironment is shut down");
			}

			for (final ResultPartition partition : producedPartitions) {
				setupPartition(partition);
			}

			// Setup the buffer pool for each buffer reader
			//获得任务的所有输入闸门
			final SingleInputGate[] inputGates = task.getAllInputGates();
			//遍历输入闸门，为它们设置缓冲池
			for (SingleInputGate gate : inputGates) {
				setupInputGate(gate);
			}
		}
	}

	@VisibleForTesting
	public void setupPartition(ResultPartition partition) throws IOException {
		BufferPool bufferPool = null;

		try {
			int maxNumberOfMemorySegments = partition.getPartitionType().isBounded() ?
				partition.getNumberOfSubpartitions() * config.networkBuffersPerChannel() +
					config.floatingNetworkBuffersPerGate() : Integer.MAX_VALUE;
			// If the partition type is back pressure-free, we register with the buffer pool for
			// callbacks to release memory.
			// 用网络缓冲池创建本地缓冲池
			bufferPool = networkBufferPool.createBufferPool(partition.getNumberOfSubpartitions(),
				maxNumberOfMemorySegments,
				partition.getPartitionType().hasBackPressure() ? Optional.empty() : Optional.of(partition));
			//将本地缓冲池注册到结果分区
			partition.registerBufferPool(bufferPool);
			//结果分区会被注册到结果分区管理器
			resultPartitionManager.registerResultPartition(partition);
		} catch (Throwable t) {
			if (bufferPool != null) {
				bufferPool.lazyDestroy();
			}

			if (t instanceof IOException) {
				throw (IOException) t;
			} else {
				throw new IOException(t.getMessage(), t);
			}
		}
	}

	@VisibleForTesting
	public void setupInputGate(SingleInputGate gate) throws IOException {
		BufferPool bufferPool = null;
		int maxNumberOfMemorySegments;
		try {
			if (config.isCreditBased()) {
				maxNumberOfMemorySegments = gate.getConsumedPartitionType().isBounded() ?
					config.floatingNetworkBuffersPerGate() : Integer.MAX_VALUE;

				// assign exclusive buffers to input channels directly and use the rest for floating buffers
				gate.assignExclusiveSegments(networkBufferPool, config.networkBuffersPerChannel());
				//为每个输入闸门设置本地缓冲池
				bufferPool = networkBufferPool.createBufferPool(0, maxNumberOfMemorySegments);
			} else {
				maxNumberOfMemorySegments = gate.getConsumedPartitionType().isBounded() ?
					gate.getNumberOfInputChannels() * config.networkBuffersPerChannel() +
						config.floatingNetworkBuffersPerGate() : Integer.MAX_VALUE;

				//为每个输入闸门设置本地缓冲池，初始化的缓冲数为其包含的输入信道数
				bufferPool = networkBufferPool.createBufferPool(gate.getNumberOfInputChannels(),
					maxNumberOfMemorySegments);
			}
			gate.setBufferPool(bufferPool);
		} catch (Throwable t) {
			if (bufferPool != null) {
				bufferPool.lazyDestroy();
			}

			ExceptionUtils.rethrowIOException(t);
		}
	}

	public void start() throws IOException {
		synchronized (lock) {
			Preconditions.checkState(!isShutdown, "The NetworkEnvironment has already been shut down.");

			LOG.info("Starting the network environment and its components.");

			try {
				LOG.debug("Starting network connection manager");
				//启动网络连接管理器
				connectionManager.start(resultPartitionManager, taskEventPublisher);
			} catch (IOException t) {
				throw new IOException("Failed to instantiate network connection manager.", t);
			}
		}
	}

	/**
	 * Tries to shut down all network I/O components.
	 */
	public void shutdown() {
		synchronized (lock) {
			if (isShutdown) {
				return;
			}

			LOG.info("Shutting down the network environment and its components.");

			// terminate all network connections
			try {
				LOG.debug("Shutting down network connection manager");
				connectionManager.shutdown();
			}
			catch (Throwable t) {
				LOG.warn("Cannot shut down the network connection manager.", t);
			}

			// shutdown all intermediate results
			try {
				LOG.debug("Shutting down intermediate result partition manager");
				resultPartitionManager.shutdown();
			}
			catch (Throwable t) {
				LOG.warn("Cannot shut down the result partition manager.", t);
			}

			// make sure that the global buffer pool re-acquires all buffers
			networkBufferPool.destroyAllBufferPools();

			// destroy the buffer pool
			try {
				networkBufferPool.destroy();
			}
			catch (Throwable t) {
				LOG.warn("Network buffer pool did not shut down properly.", t);
			}

			isShutdown = true;
		}
	}

	public boolean isShutdown() {
		synchronized (lock) {
			return isShutdown;
		}
	}
}
