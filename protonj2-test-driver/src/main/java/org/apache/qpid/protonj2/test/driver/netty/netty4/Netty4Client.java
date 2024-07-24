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
package org.apache.qpid.protonj2.test.driver.netty.netty4;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.test.driver.ProtonTestClientOptions;
import org.apache.qpid.protonj2.test.driver.netty.NettyClient;
import org.apache.qpid.protonj2.test.driver.netty.NettyEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Self contained Netty client implementation that provides a base for more
 * complex client implementations to use as the IO layer.
 */
public final class Netty4Client implements NettyClient {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String AMQP_SUB_PROTOCOL = "amqp";
    private static final int SHUTDOWN_TIMEOUT = 50;

    private Netty4EventLoop eventLoop;
    private Bootstrap bootstrap;
    private EventLoopGroup group;
    private Channel channel;
    private String host;
    private int port;
    private boolean wsCompressionRequest;
    private boolean wsCompressionResponse;
    protected volatile IOException failureCause;
    private final ProtonTestClientOptions options;
    private volatile SslHandler sslHandler;
    protected final AtomicBoolean connected = new AtomicBoolean();
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final CountDownLatch connectedLatch = new CountDownLatch(1);

    private final Consumer<ByteBuffer> inputConsumer;
    private final Runnable connectedRunnable;

    public Netty4Client(ProtonTestClientOptions options, Runnable connectedRunnable, Consumer<ByteBuffer> inputConsumer) {
        Objects.requireNonNull(options);
        Objects.requireNonNull(inputConsumer);
        Objects.requireNonNull(connectedRunnable);

        this.options = options;
        this.connectedRunnable = connectedRunnable;
        this.inputConsumer = inputConsumer;
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            connected.set(false);
            connectedLatch.countDown();
            if (channel != null) {
                try {
                    if (!channel.close().await(10, TimeUnit.SECONDS)) {
                        LOG.info("Channel close timed out waiting for result");
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    LOG.debug("Close of channel interrupted while awaiting result");
                }
            }

            if (group != null && !group.isShutdown()) {
                group.shutdownGracefully(0, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
                try {
                    if (eventLoop != null && eventLoop.inEventLoop()) {
                        // If scripted close we might be inside the event loop and
                        // we cannot wait in that case.
                        return;
                    }

                    if (!group.awaitTermination(2 * SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
                        LOG.trace("Connection IO Event Loop shutdown failed to complete in allotted time");
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    LOG.debug("Shutdown of netty event loop interrupted while awaiting result");
                }
            }
        }
    }

    @Override
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

        group = new NioEventLoopGroup(1);
        bootstrap = new Bootstrap().channel(NioSocketChannel.class).group(group);
        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            public void initChannel(Channel transportChannel) throws Exception {
                channel = transportChannel;
                eventLoop = new Netty4EventLoop(channel.eventLoop());
                configureChannel(transportChannel);
            }
        });

        configureNetty(bootstrap, options);

        bootstrap.connect(host, port).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
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

    @Override
    public NettyEventLoop eventLoop() {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        return eventLoop;
    }

    @Override
    public void write(ByteBuffer buffer) {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        channel.writeAndFlush(Unpooled.wrappedBuffer(buffer).asReadOnly());
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public boolean isSecure() {
        return options.isSecure();
    }

    @Override
    public boolean isWSCompressionActive() {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        return wsCompressionRequest && wsCompressionResponse;
    }

    @Override
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

    protected class NettyClientInboundHandler extends ChannelInboundHandlerAdapter {

        private final WebSocketClientHandshaker handshaker;
        private ScheduledFuture<?> handshakeTimeoutFuture;

        public NettyClientInboundHandler() {
            if (options.isUseWebSockets()) {
                DefaultHttpHeaders headers = new DefaultHttpHeaders();

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
                        Netty4Client.this.handleTransportFailure(channel, new IOException("WebSocket handshake timed out"));
                    }
                }, options.getConnectTimeout(), TimeUnit.MILLISECONDS);
            }

            // In the Secure case we need to let the handshake complete before we
            // trigger the connected event.
            if (!isSecure()) {
                if (!options.isUseWebSockets()) {
                    handleConnected(context.channel());
                    context.fireChannelActive();
                }
            } else {
                SslHandler sslHandler = context.pipeline().get(SslHandler.class);
                sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(Future<Channel> future) throws Exception {
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
                handshakeTimeoutFuture.cancel(false);
            }

            handleTransportFailure(context.channel(), new IOException("Remote closed connection unexpectedly"));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
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
                    if (handshakeTimeoutFuture.cancel(false)) {
                        handleConnected(ch);
                    }

                    return;
                }

                // We shouldn't get this since we handle the handshake previously.
                if (message instanceof FullHttpResponse) {
                    FullHttpResponse response = (FullHttpResponse) message;
                    throw new IllegalStateException(
                        "Unexpected FullHttpResponse (getStatus=" + response.status() +
                        ", content=" + response.content().toString(StandardCharsets.UTF_8) + ')');
                }

                WebSocketFrame frame = (WebSocketFrame) message;
                if (frame instanceof TextWebSocketFrame) {
                    TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
                    LOG.warn("WebSocket Client received message: " + textFrame.text());
                    ctx.fireExceptionCaught(new IOException("Received invalid frame over WebSocket."));
                } else if (frame instanceof BinaryWebSocketFrame) {
                    BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
                    LOG.trace("WebSocket Client received data: {} bytes", binaryFrame.content().readableBytes());
                    ctx.fireChannelRead(binaryFrame.content());
                } else if (frame instanceof ContinuationWebSocketFrame) {
                    ContinuationWebSocketFrame continuationFrame = (ContinuationWebSocketFrame) frame;
                    LOG.trace("WebSocket Client received data continuation: {} bytes", continuationFrame.content().readableBytes());
                    ctx.fireChannelRead(continuationFrame.content());
                } else if (frame instanceof PingWebSocketFrame) {
                    LOG.trace("WebSocket Client received ping, response with pong");
                    ch.write(new PongWebSocketFrame(frame.content()));
                } else if (frame instanceof CloseWebSocketFrame) {
                    LOG.trace("WebSocket Client received closing");
                    ch.close();
                }
            } else {
                ctx.fireChannelRead(message);
            }
        }
    }

    private class NettyClientOutboundHandler extends ChannelOutboundHandlerAdapter  {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            LOG.trace("NettyServerHandler: Channel write: {}", msg);
            if (options.isUseWebSockets() && msg instanceof ByteBuf) {
                if (options.isFragmentWrites()) {
                    ByteBuf orig = (ByteBuf) msg;
                    int origIndex = orig.readerIndex();
                    int split = orig.readableBytes()/2;

                    ByteBuf part1 = orig.copy(origIndex, split);
                    LOG.trace("NettyClientOutboundHandler: Part1: {}", part1);
                    orig.readerIndex(origIndex + split);
                    LOG.trace("NettyClientOutboundHandler: Part2: {}", orig);

                    BinaryWebSocketFrame frame1 = new BinaryWebSocketFrame(false, 0, part1);
                    ctx.writeAndFlush(frame1);
                    ContinuationWebSocketFrame frame2 = new ContinuationWebSocketFrame(true, 0, orig);
                    ctx.write(frame2, promise);
                } else {
                    BinaryWebSocketFrame frame = new BinaryWebSocketFrame((ByteBuf) msg);
                    ctx.write(frame, promise);
                }
            } else {
                ctx.write(msg, promise);
            }
        }
    }

    private class ClientWSCompressionObserver extends ChannelDuplexHandler  {

        final String WS_EXTENSIONS_SECTION = "sec-websocket-extensions";
        final String WS_PERMESSAGE_DEFLATE = "permessage-deflate";
        final String WS_UPGRADE = "upgrade";

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (message instanceof FullHttpResponse) {
                FullHttpResponse request = (FullHttpResponse) message;
                HttpHeaders headers = request.headers();

                if (headers.contains(WS_UPGRADE) && headers.contains(WS_EXTENSIONS_SECTION)) {
                    wsCompressionRequest = headers.get(WS_EXTENSIONS_SECTION).contains(WS_PERMESSAGE_DEFLATE);
                }
            }

            ctx.fireChannelRead(message);
        }

        @Override
        public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) throws Exception {
            if (message instanceof FullHttpRequest) {
                FullHttpRequest response = (FullHttpRequest) message;
                HttpHeaders headers = response.headers();

                if (headers.contains(WS_UPGRADE) && headers.contains(WS_EXTENSIONS_SECTION)) {
                    wsCompressionResponse = headers.get(WS_EXTENSIONS_SECTION).contains(WS_PERMESSAGE_DEFLATE);
                }
            }

            context.write(message, promise);
        }
    }

    //----- Internal Client implementation API

    protected ChannelHandler getClientHandler() {
        return new SimpleChannelInboundHandler<ByteBuf>() {

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf input) throws Exception {
                LOG.trace("AMQP Test Client Channel read: {}", input);

                // Driver processes new data and may produce output based on this.
                try {
                    final ByteBuffer copy = ByteBuffer.allocate(input.readableBytes());
                    input.readBytes(copy);
                    inputConsumer.accept(copy.flip().asReadOnlyBuffer());
                } catch (Throwable e) {
                    LOG.error("Closed AMQP Test client channel due to error: ", e);
                    ctx.channel().close();
                }
            }
        };
    }

    protected EventLoop getEventLoop() {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        return channel.eventLoop();
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
            channel.pipeline().addLast(new HttpObjectAggregator(8192));
            if (options.isWebSocketCompression()) {
                channel.pipeline().addLast(new ClientWSCompressionObserver());
                channel.pipeline().addLast(WebSocketClientCompressionHandler.INSTANCE);
            }
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
        connectedRunnable.run();
    }

    protected void handleTransportFailure(Channel failedChannel, Throwable cause) {
        if (!closed.get()) {
            LOG.trace("Channel indicates connection failure! Channel is {}", failedChannel);
            failureCause = new IOException(cause);
            channel = failedChannel;
            connected.set(false);
            connectedLatch.countDown();
        } else {
            LOG.trace("Closed Channel signaled that the channel ended: {}", channel);
        }
    }
}
