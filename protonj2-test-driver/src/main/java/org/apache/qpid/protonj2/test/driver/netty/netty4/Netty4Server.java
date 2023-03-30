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

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.apache.qpid.protonj2.test.driver.ProtonTestServerOptions;
import org.apache.qpid.protonj2.test.driver.netty.NettyEventLoop;
import org.apache.qpid.protonj2.test.driver.netty.NettyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler.HandshakeComplete;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Base Server implementation used to create Netty based server implementations for
 * unit testing aspects of the client code.
 */
public final class Netty4Server implements NettyServer {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final int PORT = Integer.parseInt(System.getProperty("port", "5672"));
    static final String WEBSOCKET_PATH = "/";
    static final int DEFAULT_MAX_FRAME_SIZE = 65535;

    private Netty4EventLoop eventLoop;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private Channel clientChannel;
    private final ProtonTestServerOptions options;
    private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    private String webSocketPath = WEBSOCKET_PATH;
    private volatile SslHandler sslHandler;
    private volatile HandshakeComplete handshakeComplete;
    private final CountDownLatch handshakeCompletion = new CountDownLatch(1);

    private final AtomicBoolean started = new AtomicBoolean();

    private final Consumer<ByteBuffer> inputConsumer;
    private final Runnable connectedRunnable;
    private final Runnable disconnectedRunnable;

    public Netty4Server(ProtonTestServerOptions options, Runnable connectedRunnable, Runnable disconnectedRunnable, Consumer<ByteBuffer> inputConsumer) {
        Objects.requireNonNull(options);
        Objects.requireNonNull(inputConsumer);
        Objects.requireNonNull(connectedRunnable);
        Objects.requireNonNull(disconnectedRunnable);

        this.options = options;
        this.connectedRunnable = connectedRunnable;
        this.inputConsumer = inputConsumer;
        this.disconnectedRunnable = disconnectedRunnable;
    }

    @Override
    public boolean isSecureServer() {
        return options.isSecure();
    }

    @Override
    public boolean isAcceptingConnections() {
        return serverChannel != null && serverChannel.isOpen();
    }

    @Override
    public boolean hasSecureConnection() {
        return sslHandler != null;
    }

    @Override
    public boolean hasClientConnection() {
        return clientChannel != null && clientChannel.isOpen();
    }

    @Override
    public int getClientPort() {
        Objects.requireNonNull(clientChannel);
        return (((InetSocketAddress) clientChannel.remoteAddress()).getPort());
    }

    @Override
    public boolean isPeerVerified() {
        try {
            if (hasSecureConnection()) {
                return sslHandler.engine().getSession().getPeerPrincipal() != null;
            } else {
                return false;
            }
        } catch (SSLPeerUnverifiedException unverified) {
            return false;
        }
    }

    @Override
    public SSLEngine getConnectionSSLEngine() {
        if (hasSecureConnection()) {
            return sslHandler.engine();
        } else {
            return null;
        }
    }

    @Override
    public boolean isWebSocketServer() {
        return options.isUseWebSockets();
    }

    @Override
    public String getWebSocketPath() {
        return webSocketPath;
    }

    @Override
    public void setWebSocketPath(String webSocketPath) {
        this.webSocketPath = webSocketPath;
    }

    @Override
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public boolean awaitHandshakeCompletion(long delayMs) throws InterruptedException {
        return handshakeCompletion.await(delayMs, TimeUnit.MILLISECONDS);
    }

    public HandshakeComplete getHandshakeComplete() {
        return handshakeComplete;
    }

    @Override
    public URI getConnectionURI(String queryString) throws Exception {
        if (!started.get()) {
            throw new IllegalStateException("Cannot get URI of non-started server");
        }

        int port = getServerPort();

        String scheme;
        String path;

        if (isWebSocketServer()) {
            if (isSecureServer()) {
                scheme = "amqpwss";
            } else {
                scheme = "amqpws";
            }
        } else {
            if (isSecureServer()) {
                scheme = "amqps";
            } else {
                scheme = "amqp";
            }
        }

        if (isWebSocketServer()) {
            path = getWebSocketPath();
        } else {
            path = null;
        }

        if (queryString != null && queryString.startsWith("?")) {
            queryString = queryString.substring(1);
        }

        return new URI(scheme, null, "localhost", port, path, queryString, null);
    }

    @Override
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            // Configure the server to basic NIO type channels
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workerGroup);
            server.channel(NioServerSocketChannel.class);
            server.option(ChannelOption.SO_BACKLOG, 100);
            server.handler(new LoggingHandler(LogLevel.INFO));
            server.childHandler(new ChannelInitializer<Channel>() {

                @Override
                public void initChannel(Channel ch) throws Exception {
                    // Don't accept any new connections.
                    if (clientChannel != null) {
                        throw new UnsupportedOperationException("Server cannot have more than one connected client at a time");
                    }

                    // Now we know who the client is
                    clientChannel = ch;

                    eventLoop = new Netty4EventLoop(ch.eventLoop());

                    if (isSecureServer()) {
                        ch.pipeline().addLast(sslHandler = SslSupport.createServerSslHandler(null, options));
                    }

                    if (options.isUseWebSockets()) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(65536));
                        ch.pipeline().addLast(new WebSocketServerProtocolHandler(getWebSocketPath(), "amqp", true, maxFrameSize));
                    }

                    ch.pipeline().addLast(new NettyServerOutboundHandler());
                    ch.pipeline().addLast(new NettyServerInboundHandler());
                    ch.pipeline().addLast(getServerHandler());
                }
            });

            // Start the server and then update the server port in case the configuration
            // was such that the server chose a free port.
            serverChannel = server.bind(options.getServerPort()).sync().channel();
            options.setServerPort(((InetSocketAddress) serverChannel.localAddress()).getPort());
        }
    }

    protected ChannelHandler getServerHandler() {
        return new SimpleChannelInboundHandler<ByteBuf>() {

            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                connectedRunnable.run();
                ctx.fireChannelActive();
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                disconnectedRunnable.run();
                ctx.fireChannelInactive();
            }

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf input) throws Exception {
                LOG.trace("AMQP Test Server Channel read: {}", input);

                // Driver processes new data and may produce output based on this.
                try {
                    final ByteBuffer copy = ByteBuffer.allocate(input.readableBytes());
                    input.readBytes(copy);
                    inputConsumer.accept(copy.flip().asReadOnlyBuffer());
                } catch (Throwable e) {
                    LOG.error("Closed AMQP Test server channel due to error: ", e);
                    ctx.channel().close();
                }
            }
        };
    }

    @Override
    public void write(ByteBuffer frame) {
        if (clientChannel == null || !clientChannel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        clientChannel.writeAndFlush(Unpooled.wrappedBuffer(frame), clientChannel.voidPromise());
    }

    @Override
    public NettyEventLoop eventLoop() {
        if (clientChannel == null || !clientChannel.isActive()) {
            throw new IllegalStateException("Channel is not connected or has closed");
        }

        return eventLoop;
    }

    @Override
    public void stopAsync() throws InterruptedException {
        if (started.compareAndSet(true, false)) {
            LOG.info("Closing channel asynchronously");
            serverChannel.close().sync();

            if (clientChannel != null) {
                clientChannel.close();
            }

            // Shut down all event loops to terminate all threads.
            int timeout = 100;
            LOG.trace("Shutting down boss group asynchronously");
            bossGroup.shutdownGracefully(0, timeout, TimeUnit.MILLISECONDS);

            LOG.trace("Shutting down worker group asynchronously");
            workerGroup.shutdownGracefully(0, timeout, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void disconnectClient() throws Exception {
        if (!started.get() || !serverChannel.isOpen()) {
            throw new IllegalStateException("Server must be currently active in order to reset");
        }

        if (clientChannel != null) {
            try {
                if (!clientChannel.close().await(10, TimeUnit.SECONDS)) {
                    LOG.info("Connected Client channel close timed out waiting for result");
                }
            } catch (InterruptedException e) {
                Thread.interrupted();
                LOG.debug("Close of connected client channel interrupted while awaiting result");
            } finally {
                clientChannel = null;
            }
        }
    }

    @Override
    public void stop() throws InterruptedException {
        if (started.compareAndSet(true, false)) {
            LOG.info("Syncing channel close");
            serverChannel.close().syncUninterruptibly();

            if (clientChannel != null) {
                try {
                    if (!clientChannel.close().await(10, TimeUnit.SECONDS)) {
                        LOG.info("Connected Client channel close timed out waiting for result");
                    }
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    LOG.debug("Close of connected client channel interrupted while awaiting result");
                }
            }

            // Shut down all event loops to terminate all threads.
            int timeout = 100;
            LOG.trace("Shutting down boss group");
            bossGroup.shutdownGracefully(0, timeout, TimeUnit.MILLISECONDS).awaitUninterruptibly(timeout);
            LOG.trace("Boss group shut down");

            LOG.trace("Shutting down worker group");
            workerGroup.shutdownGracefully(0, timeout, TimeUnit.MILLISECONDS).awaitUninterruptibly(timeout);
            LOG.trace("Worker group shut down");
        }
    }

    @Override
    public void close() throws InterruptedException {
        stop();
    }

    @Override
    public int getServerPort() {
        if (!started.get()) {
            throw new IllegalStateException("Cannot get server port of non-started server");
        }

        return options.getServerPort();
    }

    private class NettyServerOutboundHandler extends ChannelOutboundHandlerAdapter  {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            LOG.trace("NettyServerHandler: Channel write: {}", msg);
            if (isWebSocketServer() && msg instanceof ByteBuf) {
                if (options.isFragmentWrites()) {
                    ByteBuf orig = (ByteBuf) msg;
                    int origIndex = orig.readerIndex();
                    int split = orig.readableBytes()/2;

                    ByteBuf part1 = orig.copy(origIndex, split);
                    LOG.trace("NettyServerHandler: Part1: {}", part1);
                    orig.readerIndex(origIndex + split);
                    LOG.trace("NettyServerHandler: Part2: {}", orig);

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

    private class NettyServerInboundHandler extends ChannelInboundHandlerAdapter  {

        @Override
        public void userEventTriggered(ChannelHandlerContext context, Object payload) {
            if (payload instanceof HandshakeComplete) {
                handshakeComplete = (HandshakeComplete) payload;
                handshakeCompletion.countDown();
            }
        }

        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            LOG.info("NettyServerHandler -> New active channel: {}", ctx.channel());
            SslHandler handler = ctx.pipeline().get(SslHandler.class);
            if (handler != null) {
                handler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(Future<Channel> future) throws Exception {
                        LOG.info("Server -> SSL handshake completed. Succeeded: {}", future.isSuccess());
                        if (!future.isSuccess()) {
                            ctx.close();
                        }
                    }
                });
            }

            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("NettyServerHandler: channel has gone inactive: {}", ctx.channel());
            ctx.close();
            ctx.fireChannelInactive();
            Netty4Server.this.clientChannel = null;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            LOG.trace("NettyServerHandler: Channel read: {}", msg);
            if (msg instanceof WebSocketFrame) {
                WebSocketFrame frame = (WebSocketFrame) msg;
                ctx.fireChannelRead(frame.content());
            } else if (msg instanceof FullHttpRequest) {
                // Reject anything not on the WebSocket path
                FullHttpRequest request = (FullHttpRequest) msg;
                sendHttpResponse(ctx, request, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
            } else {
                // Forward anything else along to the next handler.
                ctx.fireChannelRead(msg);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.info("NettyServerHandler: NettyServerHandlerException caught on channel: {}", ctx.channel());
            // Close the connection when an exception is raised.
            cause.printStackTrace();
            ctx.close();
        }
    }

    private static void sendHttpResponse(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response) {
        // Generate an error page if response getStatus code is not OK (200).
        if (response.status().code() != 200) {
            ByteBuf buf = Unpooled.copiedBuffer(response.status().toString(), StandardCharsets.UTF_8);
            response.content().writeBytes(buf);
            buf.release();
            HttpUtil.setContentLength(response, response.content().readableBytes());
        }

        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.channel().writeAndFlush(response);
        if (!HttpUtil.isKeepAlive(request) || response.status().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    protected SslHandler getSslHandler() {
        return sslHandler;
    }
}
