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
package org.apache.qpid.protonj2.client.transport.netty5;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty5.bootstrap.Bootstrap;
import io.netty5.buffer.Buffer;
import io.netty5.channel.Channel;
import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerAdapter;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.channel.ChannelPipeline;
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
import io.netty5.util.concurrent.Future;
import io.netty5.util.concurrent.FutureListener;

/**
 * Netty based WebSockets Transport that wraps and extends the TCP Transport.
 */
public class WebSocketTransport extends TcpTransport {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketTransport.class);

    private static final String AMQP_SUB_PROTOCOL = "amqp";

    private Future<Void> handshakeTimeoutFuture;

    /**
     * Create a new {@link WebSocketTransport} instance with the given configuration.
     *
     * @param bootstrap
     *        the {@link Bootstrap} that this transport's IO is bound to.
     * @param options
     *        the {@link TransportOptions} used to configure the socket connection.
     * @param sslOptions
     *        the {@link SslOptions} to use if the options indicate SSL is enabled.
     */
    public WebSocketTransport(Bootstrap bootstrap, TransportOptions options, SslOptions sslOptions) {
        super(bootstrap, options, sslOptions);
    }

    @Override
    public URI getRemoteURI() {
        if (host != null) {
            try {
                return new URI(getScheme(), null, host, port, options.webSocketPath(), null, null);
            } catch (URISyntaxException e) {
            }
        }

        return null;
    }

    @Override
    protected ChannelHandler createChannelHandler() {
        return new NettyWebSocketTransportHandler();
    }

    @Override
    protected void addAdditionalHandlers(ChannelPipeline pipeline) {
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new HttpObjectAggregator<DefaultHttpContent>(8192));
    }

    @Override
    protected void handleConnected(Channel channel) throws Exception {
        LOG.trace("Channel has become active, awaiting WebSocket handshake! Channel is {}", channel);
    }

    @Override
    protected String getScheme() {
        return isSecure() ? "wss" : "ws";
    }

    //----- Handle connection events -----------------------------------------//

    private class OutputBufferToBinaryFrameHandler extends ChannelHandlerAdapter {

        @Override
        public Future<Void> write(ChannelHandlerContext ctx, Object msg) {
            return ctx.write(new BinaryWebSocketFrame((Buffer) msg));
        }
    }

    private class NettyWebSocketTransportHandler extends NettyDefaultHandler<Object> implements ChannelHandler {

        private final WebSocketClientHandshaker handshaker;

        public NettyWebSocketTransportHandler() {
            HttpHeaders headers = HttpHeaders.newHeaders();

            options.webSocketHeaders().forEach((key, value) -> {
                headers.set(key, value);
            });

            handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                getRemoteURI(), WebSocketVersion.V13, AMQP_SUB_PROTOCOL,
                true, headers, options.webSocketMaxFrameSize());
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            if (handshakeTimeoutFuture != null) {
                handshakeTimeoutFuture.cancel();
            }

            super.channelInactive(context);
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            handshaker.handshake(context.channel()).addListener(new FutureListener<Void>() {

                @Override
                public void operationComplete(Future<? extends Void> future) throws Exception {
                    if (future.isSuccess()) {
                        // Now we can add a transformer for outbound buffers that will produce
                        // binary websocket frame wrappers to ensure our writes are encoded.
                        context.pipeline().addLast(new OutputBufferToBinaryFrameHandler());
                    }
                }
            });

            handshakeTimeoutFuture = context.executor().schedule(()-> {
                LOG.trace("WebSocket handshake timed out! Channel is {}", context.channel());
                if (!handshaker.isHandshakeComplete()) {
                    WebSocketTransport.super.handleTransportFailure(channel, new IOException("WebSocket handshake timed out"));
                }
            }, getTransportOptions().connectTimeout(), TimeUnit.MILLISECONDS);

            super.channelActive(context);
        }

        @SuppressWarnings("resource")
        @Override
        protected void messageReceived(ChannelHandlerContext ctx, Object message) throws Exception {
            LOG.trace("New data read: incoming: {}", message);

            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ch, (FullHttpResponse) message);
                LOG.trace("WebSocket Client connected! {}", ctx.channel());
                // Now trigger super processing as we are really connected.
                if (handshakeTimeoutFuture.cancel()) {
                    WebSocketTransport.super.handleConnected(ch);
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
                LOG.warn("WebSocket Client received message: {}", textFrame.text());
                ctx.fireChannelExceptionCaught(new IOException("Received invalid frame over WebSocket."));
            } else if (frame instanceof BinaryWebSocketFrame) {
                BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
                LOG.trace("WebSocket Client received data: {} bytes", binaryFrame.binaryData().readableBytes());
                dispatchReadBuffer(binaryFrame.binaryData());
            } else if (frame instanceof ContinuationWebSocketFrame) {
                ContinuationWebSocketFrame continuationFrame = (ContinuationWebSocketFrame) frame;
                LOG.trace("WebSocket Client received data continuation: {} bytes", continuationFrame.binaryData().readableBytes());
                dispatchReadBuffer(continuationFrame.binaryData());
            } else if (frame instanceof PingWebSocketFrame) {
                LOG.trace("WebSocket Client received ping, response with pong");
                ch.write(new PongWebSocketFrame(frame.binaryData()));
            } else if (frame instanceof CloseWebSocketFrame) {
                LOG.trace("WebSocket Client received closing");
                ch.close();
            }
        }
    }
}
