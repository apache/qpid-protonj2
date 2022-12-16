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
package org.apache.qpid.protonj2.test.driver.netty;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.test.driver.ProtonTestClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty5.bootstrap.Bootstrap;
import io.netty5.buffer.Buffer;
import io.netty5.buffer.BufferAllocator;
import io.netty5.channel.Channel;
import io.netty5.channel.ChannelFutureListeners;
import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerAdapter;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.channel.ChannelInitializer;
import io.netty5.channel.ChannelOption;
import io.netty5.channel.EventLoop;
import io.netty5.channel.EventLoopGroup;
import io.netty5.channel.MultithreadEventLoopGroup;
import io.netty5.channel.nio.NioHandler;
import io.netty5.channel.socket.nio.NioSocketChannel;
import io.netty5.handler.codec.http.DefaultHttpContent;
import io.netty5.handler.codec.http.FullHttpResponse;
import io.netty5.handler.codec.http.HttpClientCodec;
import io.netty5.handler.codec.http.HttpObjectAggregator;
import io.netty5.handler.codec.http.headers.HttpHeaders;
import io.netty5.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty5.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty5.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty5.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty5.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty5.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty5.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty5.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty5.handler.codec.http.websocketx.WebSocketFrame;
import io.netty5.handler.codec.http.websocketx.WebSocketVersion;
import io.netty5.handler.logging.LoggingHandler;
import io.netty5.handler.ssl.SslHandler;
import io.netty5.util.concurrent.Future;
import io.netty5.util.concurrent.FutureListener;

/**
 * Self contained Netty client implementation that provides a base for more
 * complex client implementations to use as the IO layer.
 */
public abstract class NettyClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    private static final String AMQP_SUB_PROTOCOL = "amqp";

    private Bootstrap bootstrap;
    private EventLoopGroup group;
    private Channel channel;
    private String host;
    private int port;
    protected volatile IOException failureCause;
    private final ProtonTestClientOptions options;
    private volatile SslHandler sslHandler;
    protected final AtomicBoolean connected = new AtomicBoolean();
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final CountDownLatch connectedLatch = new CountDownLatch(1);

    public NettyClient(ProtonTestClientOptions options) {
        this.options = options;
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            connected.set(false);
            connectedLatch.countDown();
            if (channel != null) {
                try {
                    if (!channel.close().asStage().await(10, TimeUnit.SECONDS)) {
                        LOG.info("Channel close timed out waiting for result");
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    LOG.debug("Close of channel interrupted while awaiting result");
                }
            }
        }
    }

    public void connect(String host, int port) throws IOException {
        if (closed.get()) {
            throw new IllegalStateException("Netty client has already been closed");
        }

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Transport host value cannot be null");
        }

        this.host = host;

        if (port > 0) {
            this.port = port;
        } else {
            if (options.isSecure()) {
                this.port = ProtonTestClientOptions.DEFAULT_SSL_PORT;
            } else {
                this.port = ProtonTestClientOptions.DEFAULT_TCP_PORT;
            }
        }

        group = new MultithreadEventLoopGroup(1, NioHandler.newFactory());
        bootstrap = new Bootstrap().channel(NioSocketChannel.class).group(group);
        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel transportChannel) throws Exception {
                channel = transportChannel;
                configureChannel(transportChannel);
            }
        });

        configureNetty(bootstrap, options);

        bootstrap.connect(host, port).addListener(channel, ChannelFutureListeners.FIRE_EXCEPTION_ON_FAILURE);
        try {
            connectedLatch.await();
        } catch (InterruptedException e) {
            Thread.interrupted();
        }

        if (!connected.get()) {
            if (failureCause != null) {
                throw failureCause;
            } else {
                throw new IOException("Netty client was closed before a connection was established.");
            }
        }
    }

    public EventLoop eventLoop() {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        return channel.executor();
    }

    public void write(ByteBuffer buffer) {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        channel.writeAndFlush(BufferAllocator.onHeapUnpooled().copyOf(buffer).makeReadOnly());
    }

    public boolean isConnected() {
        return connected.get();
    }

    public boolean isSecure() {
        return options.isSecure();
    }

    public URI getRemoteURI() {
        if (host != null) {
            try {
                if (options.isUseWebSockets()) {
                    return new URI(options.isSecure() ? "wss" : "ws", null, host, port, options.getWebSocketPath(), null, null);
                } else {
                    return new URI(options.isSecure() ? "ssl" : "tcp", null, host, port, null, null, null);
                }
            } catch (URISyntaxException e) {
            }
        }

        return null;
    }

    //----- Default implementation of Netty handler

    protected class NettyClientInboundHandler implements ChannelHandler {

        private final WebSocketClientHandshaker handshaker;
        private Future<Void> handshakeTimeoutFuture;

        public NettyClientInboundHandler() {
            if (options.isUseWebSockets()) {
                HttpHeaders headers = HttpHeaders.newHeaders();

                options.getHttpHeaders().forEach((key, value) -> {
                    headers.set(key, value);
                });

                handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                    getRemoteURI(), WebSocketVersion.V13, AMQP_SUB_PROTOCOL,
                    true, headers, options.getWebSocketMaxFrameSize());
            } else {
                handshaker = null;
            }
        }

        @Override
        public final void channelRegistered(ChannelHandlerContext context) throws Exception {
            channel = context.channel();
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            if (options.isUseWebSockets()) {
                handshaker.handshake(context.channel());

                handshakeTimeoutFuture = context.executor().schedule(()-> {
                    LOG.trace("WebSocket handshake timed out! Channel is {}", context.channel());
                    if (!handshaker.isHandshakeComplete()) {
                        NettyClient.this.handleTransportFailure(channel, new IOException("WebSocket handshake timed out"));
                    }
                }, options.getConnectTimeout(), TimeUnit.MILLISECONDS);
            }

            // In the Secure case we need to let the handshake complete before we
            // trigger the connected event.
            if (!isSecure()) {
                if (!options.isUseWebSockets()) {
                    handleConnected(context.channel());
                }
            } else {
                SslHandler sslHandler = context.pipeline().get(SslHandler.class);
                sslHandler.handshakeFuture().addListener(new FutureListener<Channel>() {
                    @Override
                    public void operationComplete(Future<? extends Channel> future) throws Exception {
                        if (future.isSuccess()) {
                            LOG.trace("SSL Handshake has completed: {}", channel);
                            if (!options.isUseWebSockets()) {
                                handleConnected(channel);
                            }
                        } else {
                            LOG.trace("SSL Handshake has failed: {}", channel);
                            handleTransportFailure(channel, future.cause());
                        }
                    }
                });
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            if (handshakeTimeoutFuture != null) {
                handshakeTimeoutFuture.cancel();
            }

            handleTransportFailure(context.channel(), new IOException("Remote closed connection unexpectedly"));
        }

        @Override
        public void channelExceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
            handleTransportFailure(context.channel(), cause);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (options.isUseWebSockets()) {
                LOG.trace("New data read: incoming: {}", message);

                Channel ch = ctx.channel();
                if (!handshaker.isHandshakeComplete()) {
                    handshaker.finishHandshake(ch, (FullHttpResponse) message);
                    LOG.trace("WebSocket Client connected! {}", ctx.channel());
                    // Now trigger super processing as we are really connected.
                    if (handshakeTimeoutFuture.cancel()) {
                        handleConnected(ch);
                    }

                    return;
                }

                // We shouldn't get this since we handle the handshake previously.
                if (message instanceof FullHttpResponse) {
                    FullHttpResponse response = (FullHttpResponse) message;
                    throw new IllegalStateException(
                        "Unexpected FullHttpResponse (getStatus=" + response.status() +
                        ", content=" + response.payload().toString(StandardCharsets.UTF_8) + ')');
                }

                WebSocketFrame frame = (WebSocketFrame) message;
                if (frame instanceof TextWebSocketFrame) {
                    TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
                    LOG.warn("WebSocket Client received message: " + textFrame.text());
                    ctx.fireChannelExceptionCaught(new IOException("Received invalid frame over WebSocket."));
                } else if (frame instanceof BinaryWebSocketFrame) {
                    BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
                    LOG.trace("WebSocket Client received data: {} bytes", binaryFrame.binaryData().readableBytes());
                    ctx.fireChannelRead(binaryFrame.binaryData());
                } else if (frame instanceof ContinuationWebSocketFrame) {
                    ContinuationWebSocketFrame continuationFrame = (ContinuationWebSocketFrame) frame;
                    LOG.trace("WebSocket Client received data continuation: {} bytes", continuationFrame.binaryData().readableBytes());
                    ctx.fireChannelRead(continuationFrame.binaryData());
                } else if (frame instanceof PingWebSocketFrame) {
                    LOG.trace("WebSocket Client received ping, response with pong");
                    ch.write(new PongWebSocketFrame(frame.binaryData()));
                } else if (frame instanceof CloseWebSocketFrame) {
                    LOG.trace("WebSocket Client received closing");
                    ch.close();
                }
            } else {
                ctx.fireChannelRead(message);
            }
        }
    }

    private class NettyClientOutboundHandler extends ChannelHandlerAdapter  {

        @Override
        public Future<Void> write(ChannelHandlerContext ctx, Object msg) {
            LOG.trace("NettyServerHandler: Channel write: {}", msg);
            if (options.isUseWebSockets() && msg instanceof Buffer) {
                if (options.isFragmentWrites()) {
                    Buffer orig = (Buffer) msg;
                    int origIndex = orig.readerOffset();
                    int split = orig.readableBytes()/2;

                    Buffer part1 = orig.copy(origIndex, split);
                    LOG.trace("NettyClientOutboundHandler: Part1: {}", part1);
                    orig.readerOffset(origIndex + split);
                    LOG.trace("NettyClientOutboundHandler: Part2: {}", orig);

                    BinaryWebSocketFrame frame1 = new BinaryWebSocketFrame(false, 0, part1);
                    ctx.writeAndFlush(frame1);
                    ContinuationWebSocketFrame frame2 = new ContinuationWebSocketFrame(true, 0, orig);
                    return ctx.write(frame2);
                } else {
                    BinaryWebSocketFrame frame = new BinaryWebSocketFrame((Buffer) msg);
                    return ctx.write(frame);
                }
            } else {
                return ctx.write(msg);
            }
        }
    }

    //----- Internal Client implementation API

    protected abstract ChannelHandler getClientHandler();

    protected EventLoop getEventLoop() {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        return channel.executor();
    }

    protected SslHandler getSslHandler() {
        return sslHandler;
    }

    private void configureChannel(final Channel channel) throws Exception {
        if (isSecure()) {
            final SslHandler sslHandler;
            try {
                sslHandler = SslSupport.createClientSslHandler(getRemoteURI(), options);
            } catch (Exception ex) {
                LOG.warn("Error during initialization of channel from SSL Handler creation:");
                handleTransportFailure(channel, ex);
                throw new IOException(ex);
            }

            channel.pipeline().addLast("ssl", sslHandler);
        }

        if (options.isTraceBytes()) {
            channel.pipeline().addLast("logger", new LoggingHandler(getClass()));
        }

        if (options.isUseWebSockets()) {
            channel.pipeline().addLast(new HttpClientCodec());
            channel.pipeline().addLast(new HttpObjectAggregator<DefaultHttpContent>(8192));
        }

        channel.pipeline().addLast(new NettyClientOutboundHandler());
        channel.pipeline().addLast(new NettyClientInboundHandler());
        channel.pipeline().addLast(getClientHandler());
    }

    private void configureNetty(Bootstrap bootstrap, ProtonTestClientOptions options) {
        bootstrap.option(ChannelOption.TCP_NODELAY, options.isTcpNoDelay());
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.getConnectTimeout());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, options.isTcpKeepAlive());
        bootstrap.option(ChannelOption.SO_LINGER, options.getSoLinger());

        if (options.getSendBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_SNDBUF, options.getSendBufferSize());
        }

        if (options.getReceiveBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_RCVBUF, options.getReceiveBufferSize());
        }

        if (options.getTrafficClass() != -1) {
            bootstrap.option(ChannelOption.IP_TOS, options.getTrafficClass());
        }

        if (options.getLocalAddress() != null || options.getLocalPort() != 0) {
            if (options.getLocalAddress() != null) {
                bootstrap.localAddress(options.getLocalAddress(), options.getLocalPort());
            } else {
                bootstrap.localAddress(options.getLocalPort());
            }
        }
    }

    //----- Event Handlers which can be overridden in subclasses -------------//

    protected void handleConnected(Channel connectedChannel) {
        LOG.trace("Channel has become active! Channel is {}", connectedChannel);
        channel = connectedChannel;
        connected.set(true);
        connectedLatch.countDown();
    }

    protected void handleTransportFailure(Channel failedChannel, Throwable cause) {
        if (!closed.get()) {
            LOG.trace("Channel indicates connection failure! Channel is {}", failedChannel);
            failureCause = new IOException(cause);
            channel = failedChannel;
            connected.set(false);
            connectedLatch.countDown();
        } else {
            LOG.trace("Closed Channel signalled that the channel ended: {}", channel);
        }
    }
}
