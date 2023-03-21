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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.netty.NettyIOBuilder;
import org.apache.qpid.protonj2.test.driver.netty.NettyClient;
import org.apache.qpid.protonj2.test.driver.netty.NettyEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Client for AMQP server testing, allows for scripting the expected inputs from
 * the server and outputs from the client back to the server.
 */
public class ProtonTestClient extends ProtonTestPeer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ProtonTestClient.class);

    private final AMQPTestDriver driver;
    private final NettyClient client;

    /**
     * Creates a Socket Test Peer using all default Server options.
     */
    public ProtonTestClient() {
        this(new ProtonTestClientOptions());
    }

    @Override
    public String getPeerName() {
        return "Client";
    }

    /**
     * Creates a Test Client using the options to configure the client connection.
     *
     * @param options
     *      The options that control the behavior of the client connection.
     */
    public ProtonTestClient(ProtonTestClientOptions options) {
        this.driver = new NettyAwareAMQPTestDriver(this::processDriverOutput,
                                                   this::processDriverAssertion,
                                                   this::eventLoop);
        this.client = NettyIOBuilder.createClient(options,
                                             this::processConnectionEstablished,
                                             this::processChannelInput);
    }

    public void connect(String hostname, int port) throws IOException {
        client.connect(hostname, port);
    }

    @Override
    public AMQPTestDriver getDriver() {
        return driver;
    }

    @Override
    protected void processConnectionEstablished() {
        LOG.trace("AMQP Client connected to remote.");
        driver.handleConnectedEstablished();
    }

    @Override
    protected void processCloseRequest() {
        try {
            client.close();
        } catch (Throwable e) {
            LOG.info("Error suppressed on client stop: ", e);
        }
    }

    @Override
    protected void processDriverOutput(ByteBuffer frame) {
        LOG.trace("AMQP Client Channel writing: {}", frame);
        client.write(frame);
    }

    protected void processChannelInput(ByteBuffer input) {
        LOG.trace("AMQP Test Client Channel processing: {}", input);
        driver.accept(input);
    }

    protected void processDriverAssertion(AssertionError error) {
        LOG.trace("AMQP Test Client Closing due to error: {}", error.getMessage());
        close();
    }

    protected NettyEventLoop eventLoop() {
        return client.eventLoop();
    }

    //----- Test driver Wrapper to ensure actions occur on the event loop

    private final class NettyAwareAMQPTestDriver extends AMQPTestDriver {

        public NettyAwareAMQPTestDriver(Consumer<ByteBuffer> frameConsumer,
                                        Consumer<AssertionError> assertionConsumer,
                                        Supplier<NettyEventLoop> scheduler) {
            super(getPeerName(), frameConsumer, assertionConsumer, scheduler);
        }

        // If the send call occurs from a reaction to processing incoming data the
        // call will be on the event loop but for actions requested by the test that
        // are directed to happen immediately they will be running on the test thread
        // and so we direct the resulting action into the event loop to avoid codec or
        // other driver resources being used on two different threads.

        @Override
        public void deferAMQPFrame(int channel, DescribedType performative, ByteBuffer payload, boolean splitWrite) {
            NettyEventLoop loop = client.eventLoop();
            if (loop.inEventLoop()) {
                super.deferAMQPFrame(channel, performative, payload, splitWrite);
            } else {
                loop.execute(() -> {
                    super.deferAMQPFrame(channel, performative, payload, splitWrite);
                });
            }
        }

        @Override
        public void deferSaslFrame(int channel, DescribedType performative) {
            NettyEventLoop loop = client.eventLoop();
            if (loop.inEventLoop()) {
                super.deferSaslFrame(channel, performative);
            } else {
                loop.execute(() -> {
                    super.deferSaslFrame(channel, performative);
                });
            }
        }

        @Override
        public void deferHeader(AMQPHeader header) {
            NettyEventLoop loop = client.eventLoop();
            if (loop.inEventLoop()) {
                super.deferHeader(header);
            } else {
                loop.execute(() -> {
                    super.deferHeader(header);
                });
            }
        }

        @Override
        public void sendAMQPFrame(int channel, DescribedType performative, ByteBuffer payload, boolean splitWrite) {
            NettyEventLoop loop = client.eventLoop();
            if (loop.inEventLoop()) {
                super.sendAMQPFrame(channel, performative, payload, splitWrite);
            } else {
                loop.execute(() -> {
                    super.sendAMQPFrame(channel, performative, payload, splitWrite);
                });
            }
        }

        @Override
        public void sendSaslFrame(int channel, DescribedType performative) {
            NettyEventLoop loop = client.eventLoop();
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
            NettyEventLoop loop = client.eventLoop();
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
            NettyEventLoop loop = client.eventLoop();
            if (loop.inEventLoop()) {
                super.sendEmptyFrame(channel);
            } else {
                loop.execute(() -> {
                    super.sendEmptyFrame(channel);
                });
            }
        }
    }
}
