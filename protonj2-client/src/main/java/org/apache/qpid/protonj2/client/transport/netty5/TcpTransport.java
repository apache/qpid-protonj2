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
import java.security.Principal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponent;
import org.apache.qpid.protonj2.buffer.ProtonBufferComponentAccessor;
import org.apache.qpid.protonj2.buffer.netty.Netty5ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.netty.Netty5ToProtonBufferAdapter;
import org.apache.qpid.protonj2.buffer.netty.ProtonBufferToNetty5Adapter;
import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.apache.qpid.protonj2.client.transport.Transport;
import org.apache.qpid.protonj2.client.transport.TransportListener;
import org.apache.qpid.protonj2.client.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty5.bootstrap.Bootstrap;
import io.netty5.buffer.Buffer;
import io.netty5.channel.Channel;
import io.netty5.channel.ChannelHandler;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.channel.ChannelInitializer;
import io.netty5.channel.ChannelOption;
import io.netty5.channel.ChannelPipeline;
import io.netty5.channel.SimpleChannelInboundHandler;
import io.netty5.handler.logging.LoggingHandler;
import io.netty5.handler.ssl.SslHandler;
import io.netty5.util.concurrent.Future;
import io.netty5.util.concurrent.FutureListener;

/**
 * TCP based transport that uses Netty as the underlying IO layer.
 */
public class TcpTransport implements Transport {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransport.class);

    protected final AtomicBoolean connected = new AtomicBoolean();
    protected final AtomicBoolean closed = new AtomicBoolean();
    protected final CountDownLatch connectedLatch = new CountDownLatch(1);
    protected final TransportOptions options;
    protected final SslOptions sslOptions;
    protected final Bootstrap bootstrap;

    protected Channel channel;
    protected volatile IOException failureCause;
    protected String host;
    protected int port;
    protected TransportListener listener;
    protected Netty5ProtonBufferAllocator nettyAllocator;

    /**
     * Create a new {@link TcpTransport} instance with the given configuration.
     *
     * @param bootstrap
     *        the Netty {@link Bootstrap} that this transport's IO layer is bound to.
     * @param options
     *        the {@link TransportOptions} used to configure the socket connection.
     * @param sslOptions
     * 		  the {@link SslOptions} to use if the options indicate SSL is enabled.
     */
    public TcpTransport(Bootstrap bootstrap, TransportOptions options, SslOptions sslOptions) {
        if (options == null) {
            throw new IllegalArgumentException("Transport Options cannot be null");
        }

        if (sslOptions == null) {
            throw new IllegalArgumentException("Transport SSL Options cannot be null");
        }

        if (bootstrap == null) {
            throw new IllegalArgumentException("A transport must have an assigned Bootstrap before connect.");
        }

        this.sslOptions = sslOptions;
        this.options = options;
        this.bootstrap = bootstrap;
    }

    @Override
    public TcpTransport connect(String host, int port, TransportListener listener) throws IOException {
        if (closed.get()) {
            throw new IllegalStateException("Transport has already been closed");
        }

        if (listener == null) {
            throw new IllegalArgumentException("A transport listener must be set before connection attempts.");
        }

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Transport host value cannot be null");
        }

        if (port < 0 && options.defaultTcpPort() < 0 && (sslOptions.sslEnabled() && sslOptions.defaultSslPort() < 0)) {
            throw new IllegalArgumentException("Transport port value must be a non-negative int value or a default port configured");
        }

        this.host = host;
        this.listener = listener;

        if (port > 0) {
            this.port = port;
        } else {
            if (sslOptions.sslEnabled()) {
                this.port = sslOptions.defaultSslPort();
            } else {
                this.port = options.defaultTcpPort();
            }
        }

        bootstrap.handler(new ChannelInitializer<>() {
            @Override
            public void initChannel(Channel transportChannel) throws Exception {
                channel = transportChannel;
                nettyAllocator = new Netty5ProtonBufferAllocator(channel.bufferAllocator());

                configureChannel(transportChannel);
                try {
                    listener.transportInitialized(TcpTransport.this);
                } catch (Throwable initError) {
                    LOG.warn("Error during initialization of channel from Transport Listener");
                    handleTransportFailure(transportChannel, IOExceptionSupport.create(initError));
                    throw initError;
                }
            }
        });

        configureNetty(bootstrap, options);

        bootstrap.connect(getHost(), getPort()).addListener(new FutureListener<Channel>() {

            @Override
            public void operationComplete(Future<? extends Channel> future) throws Exception {
                if (future.isFailed()) {
                    bootstrap.config().group().execute(() -> handleTransportFailure(channel, future.cause()));
                }
            }
        });

        return this;
    }

    @Override
    public void awaitConnect() throws InterruptedException, IOException {
        connectedLatch.await();
        if (!connected.get()) {
            if (failureCause != null) {
                throw failureCause;
            } else {
                throw new IOException("Transport was closed before a connection was established.");
            }
        }
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public boolean isSecure() {
        return sslOptions.sslEnabled();
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            connected.set(false);
            connectedLatch.countDown();
            if (channel != null) {
                try {
                    channel.close().asStage().await();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        }
    }

    @Override
    public ProtonBufferAllocator getBufferAllocator() {
        return nettyAllocator;
     }

    @Override
    public TcpTransport write(ProtonBuffer output) throws IOException {
        return write(output, null);
    }

    @Override
    public TcpTransport write(ProtonBuffer output, Runnable onComplete) throws IOException {
        checkConnected(output);
        LOG.trace("Attempted write of buffer: {}", output);
        return writeOutputBuffer(output, false, onComplete);
    }

    @Override
    public TcpTransport writeAndFlush(ProtonBuffer output) throws IOException {
        return writeAndFlush(output, null);
    }

    @Override
    public TcpTransport writeAndFlush(ProtonBuffer output, Runnable onComplete) throws IOException {
        checkConnected(output);
        LOG.trace("Attempted write and flush of buffer: {}", output);
        return writeOutputBuffer(output, true, onComplete);
    }

    @Override
    public TcpTransport flush() throws IOException {
        checkConnected();
        LOG.trace("Attempted flush of pending writes");
        channel.flush();
        return this;
    }

    @Override
    public TransportListener getTransportListener() {
        return listener;
    }

    @Override
    public TransportOptions getTransportOptions() {
        return options.clone();
    }

    @Override
    public SslOptions getSslOptions() {
        return sslOptions.clone();
    }

    @Override
    public Principal getLocalPrincipal() {
        Principal result = null;

        if (isSecure()) {
            SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
            result = sslHandler.engine().getSession().getLocalPrincipal();
        }

        return result;
    }

    private TcpTransport writeOutputBuffer(final ProtonBuffer buffer, boolean flush, Runnable onComplete) {
        int writeCount = buffer.componentCount();
        Future<Void> writeFuture = null;

        try (ProtonBuffer ioBuffer = buffer; ProtonBufferComponentAccessor accessor = buffer.componentAccessor()) {
            for (ProtonBufferComponent output = accessor.firstReadable(); output != null; output = accessor.nextReadable()) {
                final Buffer nettyBuf;

                if (output instanceof Netty5ToProtonBufferAdapter) {
                    nettyBuf = ((Netty5ToProtonBufferAdapter)output).unwrapAndRelease();
                } else if (output.unwrap() instanceof Buffer) {
                    nettyBuf = ((Buffer) output.unwrap()).copy(true);
                } else {
                    nettyBuf = channel.bufferAllocator().allocate(output.getReadableBytes());
                    if (output.hasReadbleArray()) {
                        nettyBuf.writeBytes(output.getReadableArray(), output.getReadableArrayOffset(), output.getReadableBytes());
                    } else {
                        nettyBuf.writeBytes(output.getReadableBuffer());
                    }
                }

                if (--writeCount == 0 && flush) {
                    writeFuture = channel.writeAndFlush(nettyBuf);
                } else {
                    writeFuture = channel.write(nettyBuf);
                }
            }

            if (onComplete != null) {
                writeFuture.addListener(onComplete, TcpTransport::handleWriteComplete);
            }
        }

        return this;
    }

    @SuppressWarnings("unused")
    private TcpTransport writeOutputBufferAsWrappedNettyBuffer(final ProtonBuffer buffer, boolean flush, Runnable onComplete) {
        Future<Void> writeFuture = null;

        final Buffer nettyBuf = new ProtonBufferToNetty5Adapter(buffer.transfer());

        if (flush) {
            writeFuture = channel.writeAndFlush(nettyBuf);
        } else {
            writeFuture = channel.write(nettyBuf);
        }

        if (onComplete != null) {
            writeFuture.addListener(onComplete, TcpTransport::handleWriteComplete);
        }

        return this;
    }

    private static void handleWriteComplete(Runnable onComplete, Future<? extends Void> future) {
        if (future.isSuccess()) {
            onComplete.run();
        }
    }

    //----- Internal implementation details, can be overridden as needed -----//

    protected void addAdditionalHandlers(ChannelPipeline pipeline) {

    }

    protected ChannelHandler createChannelHandler() {
        return new NettyTcpTransportHandler();
    }

    //----- Event Handlers which can be overridden in subclasses -------------//

    protected void handleConnected(Channel connectedChannel) throws Exception {
        LOG.trace("Channel has become active! Channel is {}", connectedChannel);
        channel = connectedChannel;
        connected.set(true);
        listener.transportConnected(this);
        connectedLatch.countDown();
    }

    protected void handleTransportFailure(Channel failedChannel, Throwable cause) {
        if (!closed.get()) {
            LOG.trace("Transport indicates connection failure! Channel is {}", failedChannel);
            failureCause = IOExceptionSupport.create(cause);
            channel = failedChannel;
            connected.set(false);
            connectedLatch.countDown();

            LOG.trace("Firing onTransportError listener");
            if (channel.executor().inEventLoop()) {
                listener.transportError(failureCause);
            } else {
                channel.executor().execute(() -> {
                    listener.transportError(failureCause);
                });
            }
        } else {
            LOG.trace("Closed Transport signalled that the channel ended: {}", channel);
        }
    }

    //----- State change handlers and checks ---------------------------------//

    protected final void checkConnected() throws IOException {
        if (!connected.get() || !channel.isActive()) {
            throw new IOException("Cannot send to a non-connected transport.", failureCause);
        }
    }

    private void checkConnected(ProtonBuffer output) throws IOException {
        if (!connected.get() || !channel.isActive()) {
            output.close();
            throw new IOException("Cannot send to a non-connected transport.", failureCause);
        }
    }

    private void configureNetty(Bootstrap bootstrap, TransportOptions options) {
        bootstrap.option(ChannelOption.TCP_NODELAY, options.tcpNoDelay());
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.connectTimeout());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, options.tcpKeepAlive());
        bootstrap.option(ChannelOption.SO_LINGER, options.soLinger());

        if (options.sendBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_SNDBUF, options.sendBufferSize());
        }

        if (options.receiveBufferSize() != -1) {
            bootstrap.option(ChannelOption.SO_RCVBUF, options.receiveBufferSize());
        }

        if (options.trafficClass() != -1) {
            bootstrap.option(ChannelOption.IP_TOS, options.trafficClass());
        }

        if (options.localAddress() != null || options.localPort() != 0) {
            if (options.localAddress() != null) {
                bootstrap.localAddress(options.localAddress(), options.localPort());
            } else {
                bootstrap.localAddress(options.localPort());
            }
        }
    }

    private void configureChannel(final Channel channel) throws Exception {
        if (isSecure()) {
            final SslHandler sslHandler;
            try {
                sslHandler = SslSupport.createSslHandler(channel.bufferAllocator(), host, port, sslOptions);
            } catch (Exception ex) {
                LOG.warn("Error during initialization of channel from SSL Handler creation:");
                handleTransportFailure(channel, IOExceptionSupport.create(ex));
                throw IOExceptionSupport.create(ex);
            }

            channel.pipeline().addLast("ssl", sslHandler);
        }

        if (options.traceBytes()) {
            channel.pipeline().addLast("logger", new LoggingHandler(getClass()));
        }

        addAdditionalHandlers(channel.pipeline());

        channel.pipeline().addLast(createChannelHandler());
    }

    //----- Default implementation of Netty handler --------------------------//

    protected abstract class NettyDefaultHandler<E> extends SimpleChannelInboundHandler<E> {

        public NettyDefaultHandler() {
            super(false); // We will release buffer references manually.
        }

        @Override
        public final void channelRegistered(ChannelHandlerContext context) throws Exception {
            channel = context.channel();
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            // In the Secure case we need to let the handshake complete before we
            // trigger the connected event.
            if (!isSecure()) {
                handleConnected(context.channel());
            } else {
                SslHandler sslHandler = context.pipeline().get(SslHandler.class);
                sslHandler.handshakeFuture().addListener(new FutureListener<Channel>() {
                    @Override
                    public void operationComplete(Future<? extends Channel> future) throws Exception {
                        if (future.isSuccess()) {
                            LOG.trace("SSL Handshake has completed: {}", channel);
                            handleConnected(channel);
                        } else {
                            LOG.trace("SSL Handshake has failed: {}", channel);
                            handleTransportFailure(channel, future.cause());
                        }
                    }
                });
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void channelInactive(ChannelHandlerContext context) throws Exception {
            handleTransportFailure(context.channel(), new IOException("Remote closed connection unexpectedly"));
        }

        @Override
        public void channelExceptionCaught(ChannelHandlerContext context, Throwable cause) throws Exception {
            handleTransportFailure(context.channel(), cause);
        }

        protected void dispatchReadBuffer(Buffer buffer) throws Exception {
            LOG.trace("New data read: {}", buffer);

            // Wrap the buffer and make it read-only as the handlers should not be altering the
            // read bytes and if they need to they should be copying them. If the handler doesn't
            // take ownership of the incoming buffer then we will close it and the reference count
            // will be decremented here (default auto decrement has been disabled).
            final ProtonBuffer wrapped = nettyAllocator.wrap(buffer).convertToReadOnly();

            try (wrapped) {
                listener.transportRead(wrapped);
            }
        }
    }

    //----- Handle binary data over socket connections -----------------------//

    protected class NettyTcpTransportHandler extends NettyDefaultHandler<Buffer> {

        @Override
        protected void messageReceived(ChannelHandlerContext ctx, Buffer buffer) throws Exception {
            dispatchReadBuffer(buffer);
        }
    }

    @Override
    public URI getRemoteURI() {
        if (host != null) {
            try {
                return new URI(getScheme(), null, host, port, null, null, null);
            } catch (URISyntaxException e) {
            }
        }

        return null;
    }

    protected String getScheme() {
        return isSecure() ? "ssl" : "tcp";
    }
}
