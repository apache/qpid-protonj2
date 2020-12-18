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
package org.apache.qpid.protonj2.test.driver;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.net.ssl.SSLEngine;

import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.netty.NettyServer;
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
 * Netty based AMQP Test Server implementation that can handle inbound connections
 * and script the expected AMQP frame interchange that should occur for a given test.
 */
public class ProtonTestServer extends ProtonTestPeer {

    private static final Logger LOG = LoggerFactory.getLogger(ProtonTestServer.class);

    private final AMQPTestDriver driver;
    private final NettyTestDriverServer server;
    private volatile Channel channel;

    /**
     * Creates a Socket Test Peer using all default Server options.
     */
    public ProtonTestServer() {
        this(new ProtonTestServerOptions());
    }

    /**
     * Creates a Socket Test Peer using the options to configure the deployed server.
     *
     * @param options
     *      The options that control the behavior of the deployed server.
     */
    public ProtonTestServer(ProtonTestServerOptions options) {
        this.driver = new NettyAwareAMQPTestDriver(this::processDriverOutput, this::processDriverAssertion, this::eventLoop);
        this.server = new NettyTestDriverServer(options);
    }

    public void start() {
        checkClosed();
        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start server", e);
        }
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

    @Override
    public AMQPTestDriver getDriver() {
        return driver;
    }

    //----- Channel handler that drives IO for the test driver

    private final class NettyTestDriverServer extends NettyServer {

        public NettyTestDriverServer(ProtonTestServerOptions options) {
            super(options);
        }

        @Override
        protected ChannelHandler getServerHandler() {
            return new SimpleChannelInboundHandler<ByteBuf>() {

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, ByteBuf input) throws Exception {
                    LOG.trace("AMQP Test Server Channel read: {}", input);

                    try {
                        // Create a stable copy to avoid issue with retained buffer slices when input is pooled.
                        ByteBuf copy = Unpooled.buffer(input.readableBytes());
                        copy.writeBytes(input.nioBuffer());
                        input.skipBytes(input.readableBytes());

                        // Driver processes new data and may produce output based on this.
                        processChannelInput(copy);
                    } catch (Throwable e) {
                        LOG.error("Closed AMQP Test server channel due to error: ", e);
                        ctx.channel().close();
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

        public NettyAwareAMQPTestDriver(Consumer<ByteBuffer> frameConsumer, Consumer<AssertionError> assertionConsumer, Supplier<ScheduledExecutorService> scheduler) {
            super(frameConsumer, assertionConsumer, scheduler);
        }

        // If the send call occurs from a reaction to processing incoming data the
        // call will be on the event loop but for actions requested by the test that
        // are directed to happen immediately they will be running on the test thread
        // and so we direct the resulting action into the event loop to avoid codec or
        // other driver resources being used on two different threads.

        @Override
        public void sendAMQPFrame(int channel, DescribedType performative, ByteBuf payload) {
            EventLoop loop = ProtonTestServer.this.channel.eventLoop();
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
            EventLoop loop = ProtonTestServer.this.channel.eventLoop();
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
            EventLoop loop = ProtonTestServer.this.channel.eventLoop();
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
            EventLoop loop = ProtonTestServer.this.channel.eventLoop();
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

    Channel getChannel() {
        return channel;
    }

    @Override
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

        try {
            server.stop();
        } catch (Throwable e) {
            LOG.info("Error suppressed on server stop: ", e);
        }
    }

    @Override
    protected void processDriverOutput(ByteBuffer frame) {
        LOG.trace("AMQP Server Channel writing: {}", frame);
        // TODO - Error handling for failed write
        channel.writeAndFlush(Unpooled.wrappedBuffer(frame), channel.voidPromise());
    }

    protected void processDriverAssertion(AssertionError error) {
        LOG.trace("AMQP Server Closing due to error: {}", error.getMessage());
        close();
    }

    protected void processChannelInput(ByteBuf input) {
        LOG.trace("AMQP Server Channel processing: {}", input);
        driver.accept(input);
    }

    protected ScheduledExecutorService eventLoop() {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        return channel.eventLoop();
    }
}
