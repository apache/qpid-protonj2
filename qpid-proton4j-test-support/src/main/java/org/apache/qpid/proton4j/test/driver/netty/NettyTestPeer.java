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
package org.apache.qpid.proton4j.test.driver.netty;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.net.ssl.SSLEngine;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.ScriptWriter;
import org.apache.qpid.proton4j.types.DescribedType;
import org.apache.qpid.proton4j.types.transport.AMQPHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Netty based Test Peer implementation.
 */
public class NettyTestPeer extends ScriptWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTestPeer.class);

    private final AMQPTestDriver driver;
    private final TestDriverServer server;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Channel channel;

    /**
     * Creates a Socket Test Peer using all default Server options.
     */
    public NettyTestPeer() {
        this(new ServerOptions());
    }

    /**
     * Creates a Socket Test Peer using the options to configure the deployed server.
     *
     * @param options
     *      The options that control the behavior of the deployed server.
     */
    public NettyTestPeer(ServerOptions options) {
        this.driver = new NettyAwareAMQPTestDriver((frame) -> {
            processDriverOutput(frame);
        }, () -> eventLoop());
        this.server = new TestDriverServer(options);
    }

    public void start() {
        checkClosed();
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start server", e);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            processCloseRequest();
            try {
                server.stop();
            } catch (Throwable e) {
                LOG.info("Error suppressed on server stop: ", e);
            }
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    public URI getServerURI() {
        checkClosed();
        try {
            return server.getConnectionURI();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get connection URI: ", e);
        }
    }

    public boolean isAcceptingConnections() {
        return server.isAcceptingConnections();
    }

    public boolean isSecure() {
        return server.isSecureServer();
    }

    public boolean hasSecureConnection() {
        return server.hasSecureConnection();
    }

    public boolean isConnectionVerified() {
        return server.isPeerVerified();
    }

    public SSLEngine getConnectionSSLEngine() {
        return server.getConnectionSSLEngine();
    }

    //----- Test Completion API

    public void waitForScriptToCompleteIgnoreErrors() {
        driver.waitForScriptToCompleteIgnoreErrors();
    }

    public void waitForScriptToComplete() {
        driver.waitForScriptToComplete();
    }

    public void waitForScriptToComplete(long timeout) {
        driver.waitForScriptToComplete(timeout);
    }

    public void waitForScriptToComplete(long timeout, TimeUnit units) {
        driver.waitForScriptToComplete(timeout, units);
    }

    //----- Channel handler that drives IO for the test driver

    private final class TestDriverServer extends NettyServer {

        public TestDriverServer(ServerOptions options) {
            super(options);
        }

        @Override
        protected ChannelHandler getServerHandler() {
            return new SimpleChannelInboundHandler<ByteBuf>() {

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf input) throws Exception {
                    LOG.trace("AMQP Server Channel read: {}", input);

                    try {
                        // Create a stable copy to avoid issue with retained buffer slices when input is pooled.
                        ProtonBuffer copy = ProtonByteBufferAllocator.DEFAULT.allocate(input.readableBytes());
                        copy.writeBytes(input.nioBuffer());
                        input.skipBytes(input.readableBytes());

                        // Driver processes new data and may produce output based on this.
                        processChannelInput(copy);
                    } catch (Throwable e) {
                        LOG.error("Close test peer channel due to error: ", e);
                        ctx.channel().close();
                    } finally {
                    }
                }

                @Override
                public void channelActive(ChannelHandlerContext ctx) throws Exception {
                    channel = ctx.channel();
                    ctx.fireChannelActive();
                }
            };
        }
    }

    //----- Test driver Wrapper to ensure actions occur on the event loop

    private final class NettyAwareAMQPTestDriver extends AMQPTestDriver {

        public NettyAwareAMQPTestDriver(Consumer<ProtonBuffer> frameConsumer, Supplier<ScheduledExecutorService> scheduler) {
            super(frameConsumer, scheduler);
        }

        // If the send call occurs from a reaction to processing incoming data the
        // call will be on the event loop but for actions requested by the test that
        // are directed to happen immediately they will be running on the test thread
        // and so we direct the resulting action into the event loop to avoid codec or
        // other driver resources being used on two different threads.

        @Override
        public void sendAMQPFrame(int channel, DescribedType performative, ProtonBuffer payload) {
            EventLoop loop = NettyTestPeer.this.channel.eventLoop();
            if (loop.inEventLoop()) {
                super.sendAMQPFrame(channel, performative, payload);
            } else {
                loop.execute(() -> {
                    super.sendAMQPFrame(channel, performative, payload);
                });
            }
        }

        @Override
        public void sendSaslFrame(int channel, DescribedType performative) {
            EventLoop loop = NettyTestPeer.this.channel.eventLoop();
            if (loop.inEventLoop()) {
                super.sendSaslFrame(channel, performative);
            } else {
                loop.execute(() -> {
                    super.sendSaslFrame(channel, performative);
                });
            }
        }

        @Override
        public void sendHeader(AMQPHeader header) {
            EventLoop loop = NettyTestPeer.this.channel.eventLoop();
            if (loop.inEventLoop()) {
                super.sendHeader(header);
            } else {
                loop.execute(() -> {
                    super.sendHeader(header);
                });
            }
        }

        @Override
        public void sendEmptyFrame(int channel) {
            EventLoop loop = NettyTestPeer.this.channel.eventLoop();
            if (loop.inEventLoop()) {
                super.sendEmptyFrame(channel);
            } else {
                loop.execute(() -> {
                    super.sendEmptyFrame(channel);
                });
            }
        }
    }

    //----- Internal implementation which can be overridden

    private void checkClosed() {
        if (closed.get()) {
            throw new IllegalStateException("The test peer is closed");
        }
    }

    protected void processCloseRequest() {
        if (channel != null) {
            try {
                if (!channel.close().await(10, TimeUnit.SECONDS)) {
                    LOG.info("Channel close timed out wiating for result");
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                LOG.debug("Close of channel interrupted while awaiting result");
            }
        }
    }

    protected void processDriverOutput(ProtonBuffer frame) {
        LOG.trace("AMQP Server Channel writing: {}", frame);
        // TODO - Error handling for failed write
        // TODO - If we allow test driver to allocate io buffers we might save this copy
        //        if it can create an netty buffer wrapper.
        ByteBuf outbound = Unpooled.buffer(frame.getReadableBytes());
        outbound.writeBytes(frame.toByteBuffer());
        channel.writeAndFlush(outbound, channel.voidPromise());
    }

    protected void processChannelInput(ProtonBuffer input) {
        LOG.trace("AMQP Server Channel processing: {}", input);
        driver.accept(input);
    }

    protected ScheduledExecutorService eventLoop() {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        return channel.eventLoop();
    }

    @Override
    protected AMQPTestDriver getDriver() {
        return driver;
    }
}
