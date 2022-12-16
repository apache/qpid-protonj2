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
package org.apache.qpid.protonj2.client.transport.netty4;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.apache.qpid.protonj2.client.transport.IOContext;
import org.apache.qpid.protonj2.client.util.TrackableThreadFactory;
import org.apache.qpid.protonj2.engine.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Builder of Transport instances that will validate the build options and produce a
 * correctly configured transport based on the options set.
 */
public final class Netty4IOContext implements IOContext {

    private static final Logger LOG = LoggerFactory.getLogger(Netty4IOContext.class);

    private static final int SHUTDOWN_TIMEOUT = 50;
    private static final int ASYNC_SHUTDOWN_TIMEOUT = 100;
    private static final int ASYNC_SHUTDOWN_QUIET_PERIOD = 10;

    private final EventLoopGroup group;
    private final NettyIOScheduler scheduler = new NettyIOScheduler();
    private final Class<? extends Channel> channelClass;
    private final TransportOptions options;
    private final SslOptions sslOptions;
    private final ThreadFactory threadFactory;

    public Netty4IOContext(TransportOptions options, SslOptions ssl, String ioThreadName) {
        Objects.requireNonNull(options, "Transport Options cannot be null");
        Objects.requireNonNull(ssl, "Transport SSL Options cannot be null");

        this.options = options;
        this.sslOptions = ssl;
        this.threadFactory = new TrackableThreadFactory(ioThreadName, true);

        final String[] nativeIOPreference = options.nativeIOPreference();

        EventLoopGroup selectedGroup = null;
        Class<? extends Channel> selectedChannelClass = null;

        if (options.allowNativeIO()) {
            for (String nativeID : nativeIOPreference) {
                if (EpollSupport.NAME.equalsIgnoreCase(nativeID)) {
                    if (EpollSupport.isAvailable(options)) {
                        LOG.trace("Netty Transports will be using Epoll mode");
                        selectedGroup = EpollSupport.createGroup(1, threadFactory);
                        selectedChannelClass = EpollSupport.getChannelClass();
                        break;
                    }
                } else if (IOUringSupport.NAME.equalsIgnoreCase(nativeID)) {
                    if (IOUringSupport.isAvailable(options)) {
                        LOG.trace("Netty Transports will be using IO-Uring mode");
                        selectedGroup = IOUringSupport.createGroup(1, threadFactory);
                        selectedChannelClass = IOUringSupport.getChannelClass();
                        break;
                    }
                } else if (KQueueSupport.NAME.equalsIgnoreCase(nativeID)) {
                    if (KQueueSupport.isAvailable(options)) {
                        LOG.trace("Netty Transports will be using KQueue mode");
                        selectedGroup = KQueueSupport.createGroup(1, threadFactory);
                        selectedChannelClass = KQueueSupport.getChannelClass();
                        break;
                    }
                } else {
                    throw new IllegalArgumentException(
                        String.format("Provided preferred native transport type name: %s, is not supported.", nativeID));
                }
            }
        }

        if (selectedGroup == null) {
            LOG.trace("Netty Transports will be using NIO mode");
            selectedGroup = new NioEventLoopGroup(1, threadFactory);
            selectedChannelClass = NioSocketChannel.class;
        }

        this.group = selectedGroup;
        this.channelClass = selectedChannelClass;
    }

    @Override
    public void shutdown() {
        if (!group.isShutdown()) {
            group.shutdownGracefully(0, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
            try {
                if (!group.awaitTermination(2 * SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
                    LOG.trace("Connection IO Event Loop shutdown failed to complete in allotted time");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void shutdownAsync() {
        if (!group.isShutdown()) {
            group.shutdownGracefully(ASYNC_SHUTDOWN_QUIET_PERIOD, ASYNC_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public Scheduler ioScheduler() {
        return scheduler;
    }

    @Override
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

    public class NettyIOScheduler implements Scheduler, Executor {

        @Override
        public void execute(Runnable command) {
            group.execute(command);
        }

        @Override
        public java.util.concurrent.Future<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return group.schedule(command, delay, unit);
        }

        @Override
        public <V> java.util.concurrent.Future<V> schedule(Callable<V> task, long delay, TimeUnit unit) {
            return group.schedule(task, delay, unit);
        }

        @Override
        public java.util.concurrent.Future<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            return group.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public java.util.concurrent.Future<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return group.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        @Override
        public boolean isShutdown() {
            return group.isShutdown();
        }
    }
}
