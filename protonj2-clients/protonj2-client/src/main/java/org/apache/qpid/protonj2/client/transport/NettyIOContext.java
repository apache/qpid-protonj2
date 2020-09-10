/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.client.transport;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;

/**
 * Builder of Transport instances that will validate the build options and produce a
 * correctly configured transport based on the options set.
 */
public final class NettyIOContext {

    private static final Logger LOG = LoggerFactory.getLogger(NettyIOContext.class);

    private static final int SHUTDOWN_TIMEOUT = 50;

    private final EventLoopGroup group;
    private final Class<? extends Channel> channelClass;
    private final TransportOptions options;
    private final SslOptions sslOptions;
    private final ThreadFactory threadFactory;

    public NettyIOContext(TransportOptions options, SslOptions ssl, String ioThreadName) {
        Objects.requireNonNull(options, "Transport Options cannot be null");
        Objects.requireNonNull(ssl, "Transport SSL Options cannot be null");

        this.options = options;
        this.sslOptions = ssl;
        this.threadFactory = new IOThreadFactory(ioThreadName, true);

        final boolean useKQueue = KQueueSupport.isAvailable(options) && options.allowNativeIO();
        final boolean useEpoll = EpollSupport.isAvailable(options) && options.allowNativeIO();

        if (useKQueue) {
            LOG.trace("Netty Transports will be using KQueue mode");
            group = KQueueSupport.createGroup(1, threadFactory);
            channelClass = KQueueSupport.getChannelClass();
        } else if (useEpoll) {
            LOG.trace("Netty Transports will be using Epoll mode");
            group = EpollSupport.createGroup(1, threadFactory);
            channelClass = EpollSupport.getChannelClass();
        } else {
            LOG.trace("Netty Transports will be using NIO mode");
            group = new NioEventLoopGroup(1, threadFactory);
            channelClass = NioSocketChannel.class;
        }
    }

    public void shutdown() {
        if (!group.isShutdown()) {
            Future<?> fut = group.shutdownGracefully(0, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
            if (!fut.awaitUninterruptibly(2 * SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
                LOG.trace("Connection IO Event Loop shutdown failed to complete in allotted time");
            }
        }
    }

    public EventLoopGroup eventLoop() {
        return group;
    }

    public TcpTransport newTransport() {
        if (group.isShutdown() || group.isShuttingDown() || group.isTerminated()) {
            throw new IllegalStateException("Cannot create a Transport from a shutdown IO context");
        }

        final Bootstrap bootstrap = new Bootstrap().channel(channelClass).group(group);

        final TcpTransport transport;

        if (options.useWebSockets()) {
            transport = new WebSocketTransport(bootstrap, options, sslOptions);
        } else {
            transport = new TcpTransport(bootstrap, options, sslOptions);
        }

        return transport;
    }
}
