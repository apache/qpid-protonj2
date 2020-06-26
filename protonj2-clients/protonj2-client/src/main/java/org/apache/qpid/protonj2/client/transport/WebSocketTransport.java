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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonNettyByteBuffer;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
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
import io.netty.util.concurrent.ScheduledFuture;

/**
 * Netty based WebSockets Transport that wraps and extends the TCP Transport.
 */
public class WebSocketTransport extends TcpTransport {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketTransport.class);

    private static final String AMQP_SUB_PROTOCOL = "amqp";

    private ScheduledFuture<?> handshakeTimeoutFuture;

    /**
     * Create a new transport instance
     *
     * @param host
     *        the host name or IP address that this transport connects to.
     * @param port
     * 		  the port on the given host that this transport connects to.
     * @param options
     *        the transport options used to configure the socket connection.
     * @param sslOptions
     * 		  SSL options to use if the options indicate SSL support is enabled.
     */
    public WebSocketTransport(String host, int port, TransportOptions options, SslOptions sslOptions) {
        super(host, port, options, sslOptions);
    }

    @Override
    public WebSocketTransport write(ProtonBuffer output) throws IOException {
        checkConnected();
        int length = output.getReadableBytes();
        if (length == 0) {
            return this;
        }

        LOG.trace("Attempted write of: {} bytes", length);

        channel.write(new BinaryWebSocketFrame(toOutputBuffer(output)), channel.voidPromise());

        return this;
    }

    @Override
    public WebSocketTransport writeAndFlush(ProtonBuffer output) throws IOException {
        checkConnected();
        int length = output.getReadableBytes();
        if (length == 0) {
            return this;
        }

        LOG.trace("Attempted write and flush of: {} bytes", length);

        channel.writeAndFlush(new BinaryWebSocketFrame(toOutputBuffer(output)), channel.voidPromise());

        return this;
    }

    @Override
    protected ChannelInboundHandlerAdapter createChannelHandler() {
        return new NettyWebSocketTransportHandler();
    }

    @Override
    protected void addAdditionalHandlers(ChannelPipeline pipeline) {
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new HttpObjectAggregator(8192));
    }

    @Override
    protected void handleConnected(Channel channel) throws Exception {
        LOG.trace("Channel has become active, awaiting WebSocket handshake! Channel is {}", channel);
    }

    @Override
    protected void handleChannelInactive(Channel channel) throws Exception {
        try {
            if (handshakeTimeoutFuture != null) {
                handshakeTimeoutFuture.cancel(false);
            }
        } finally {
            super.handleChannelInactive(channel);
        }
    }

    //----- Handle connection events -----------------------------------------//

    private URI getRemoteLocation() {
        try {
            return new URI(null, null, getHost(), getPort(), options.webSocketPath(), null, null);
        } catch (URISyntaxException use) {
            throw new IllegalArgumentException(use);
        }
    }

    private class NettyWebSocketTransportHandler extends NettyDefaultHandler<Object> {

        private final WebSocketClientHandshaker handshaker;

        public NettyWebSocketTransportHandler() {
            DefaultHttpHeaders headers = new DefaultHttpHeaders();

            options.webSocketHeaders().forEach((key, value) -> {
                headers.set(key, value);
            });

            handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                getRemoteLocation(), WebSocketVersion.V13, AMQP_SUB_PROTOCOL,
                true, headers, options.webSocketMaxFrameSize());
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            handshaker.handshake(context.channel());

            handshakeTimeoutFuture = context.executor().schedule(()-> {
                LOG.trace("WebSocket handshake timed out! Channel is {}", context.channel());
                if (!handshaker.isHandshakeComplete()) {
                    WebSocketTransport.super.handleException(channel, new IOException("WebSocket handshake timed out"));
                }
            }, getTransportOptions().connectTimeout(), TimeUnit.MILLISECONDS);

            super.channelActive(context);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object message) throws Exception {
            LOG.trace("New data read: incoming: {}", message);

            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ch, (FullHttpResponse) message);
                LOG.trace("WebSocket Client connected! {}", ctx.channel());
                // Now trigger super processing as we are really connected.
                if (handshakeTimeoutFuture.cancel(false)) {
                    WebSocketTransport.super.handleConnected(ch);
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
                listener.onData(new ProtonNettyByteBuffer(binaryFrame.content()));
            } else if (frame instanceof ContinuationWebSocketFrame) {
                ContinuationWebSocketFrame continuationFrame = (ContinuationWebSocketFrame) frame;
                LOG.trace("WebSocket Client received data continuation: {} bytes", continuationFrame.content().readableBytes());
                listener.onData(new ProtonNettyByteBuffer(continuationFrame.content()));
            } else if (frame instanceof PingWebSocketFrame) {
                LOG.trace("WebSocket Client received ping, response with pong");
                ch.write(new PongWebSocketFrame(frame.content()));
            } else if (frame instanceof CloseWebSocketFrame) {
                LOG.trace("WebSocket Client received closing");
                ch.close();
            }
        }
    }
}
