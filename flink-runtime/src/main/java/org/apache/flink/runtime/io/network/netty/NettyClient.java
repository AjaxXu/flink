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

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * NettyClient的主要职责是初始化Netty客户端的核心对象，并根据NettyProtocol配置用于客户端事件处理的ChannelPipeline
 * NettyClient并不用于发起远程结果子分区请求，该工作将由PartitionRequestClient完成
 */
class NettyClient {

	private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

	private final NettyConfig config;

	private NettyProtocol protocol;

	private Bootstrap bootstrap;

	@Nullable
	private SSLHandlerFactory clientSSLFactory;

	NettyClient(NettyConfig config) {
		this.config = config;
	}

	/**
	 * 1.创建Bootstrap对象用来引导启动客户端
	 * 2.创建NioEventLoopGroup或EpollEventLoopGroup对象并设置到Bootstrap中
	 * 3.进行一系列配置
	 */
	void init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) throws IOException {
		checkState(bootstrap == null, "Netty client has already been initialized.");

		this.protocol = protocol;

		final long start = System.nanoTime();

		// 创建Bootstrap对象用来引导启动客户端
		bootstrap = new Bootstrap();

		// --------------------------------------------------------------------
		// Transport-specific configuration
		// --------------------------------------------------------------------

		// 创建NioEventLoopGroup或EpollEventLoopGroup对象并设置到Bootstrap中
		switch (config.getTransportType()) {
			case NIO:
				initNioBootstrap();
				break;

			case EPOLL:
				// Netty自版本4.0.16开始，对于Linux系统提供原生的套接字通信传输支持（也即，epoll机制，借助于JNI调用），
				// 这种传输机制拥有更高的性能.只支持linux 2.6以上
				initEpollBootstrap();
				break;

			case AUTO:
				if (Epoll.isAvailable()) {
					initEpollBootstrap();
					LOG.info("Transport type 'auto': using EPOLL.");
				}
				else {
					initNioBootstrap();
					LOG.info("Transport type 'auto': using NIO.");
				}
		}

		// --------------------------------------------------------------------
		// Configuration
		// --------------------------------------------------------------------

		bootstrap.option(ChannelOption.TCP_NODELAY, true);
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

		// Timeout for new connections
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getClientConnectTimeoutSeconds() * 1000);

		// Pooled allocator for Netty's ByteBuf instances
		bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);

		// Receive and send buffer size
		int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
		if (receiveAndSendBufferSize > 0) {
			bootstrap.option(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
			bootstrap.option(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
		}

		try {
			clientSSLFactory = config.createClientSSLEngineFactory();
		} catch (Exception e) {
			throw new IOException("Failed to initialize SSL Context for the Netty client", e);
		}

		final long duration = (System.nanoTime() - start) / 1_000_000;
		LOG.info("Successful initialization (took {} ms).", duration);
	}

	NettyConfig getConfig() {
		return config;
	}

	Bootstrap getBootstrap() {
		return bootstrap;
	}

	void shutdown() {
		final long start = System.nanoTime();

		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
			bootstrap = null;
		}

		final long duration = (System.nanoTime() - start) / 1_000_000;
		LOG.info("Successful shutdown (took {} ms).", duration);
	}

	private void initNioBootstrap() {
		// Add the server port number to the name in order to distinguish
		// multiple clients running on the same host.
		String name = NettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + config.getServerPort() + ")";

		NioEventLoopGroup nioGroup = new NioEventLoopGroup(config.getClientNumThreads(), NettyServer.getNamedThreadFactory(name));
		bootstrap.group(nioGroup).channel(NioSocketChannel.class);
	}

	private void initEpollBootstrap() {
		// Add the server port number to the name in order to distinguish
		// multiple clients running on the same host.
		String name = NettyConfig.CLIENT_THREAD_GROUP_NAME + " (" + config.getServerPort() + ")";

		EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(config.getClientNumThreads(), NettyServer.getNamedThreadFactory(name));
		bootstrap.group(epollGroup).channel(EpollSocketChannel.class);
	}

	// ------------------------------------------------------------------------
	// Client connections
	// ------------------------------------------------------------------------

	ChannelFuture connect(final InetSocketAddress serverSocketAddress) {
		checkState(bootstrap != null, "Client has not been initialized yet.");

		// --------------------------------------------------------------------
		// Child channel pipeline for accepted connections
		// --------------------------------------------------------------------

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel channel) throws Exception {

				// SSL handler should be added first in the pipeline
				if (clientSSLFactory != null) {
					SslHandler sslHandler = clientSSLFactory.createNettySSLHandler(
							channel.alloc(),
							serverSocketAddress.getAddress().getCanonicalHostName(),
							serverSocketAddress.getPort());
					channel.pipeline().addLast("ssl", sslHandler);
				}
				channel.pipeline().addLast(protocol.getClientChannelHandlers());
			}
		});

		try {
			// 调用Bootstrap.connect()来连接服务器
			return bootstrap.connect(serverSocketAddress);
		}
		catch (ChannelException e) {
			if ((e.getCause() instanceof java.net.SocketException &&
					e.getCause().getMessage().equals("Too many open files")) ||
				(e.getCause() instanceof ChannelException &&
						e.getCause().getCause() instanceof java.net.SocketException &&
						e.getCause().getCause().getMessage().equals("Too many open files")))
			{
				throw new ChannelException(
						"The operating system does not offer enough file handles to open the network connection. " +
								"Please increase the number of available file handles.", e.getCause());
			}
			else {
				throw e;
			}
		}
	}
}
