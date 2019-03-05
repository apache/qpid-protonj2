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
package org.apache.qpid.proton4j.engine.test;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;

/**
 * Test driver object used to drive inputs and inspect outputs of an Engine.
 */
public class EngineTestDriver {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(EngineTestDriver.class);

    private static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    private static final Symbol EXTERNAL = Symbol.valueOf("EXTERNAL");
    private static final Symbol PLAIN = Symbol.valueOf("PLAIN");

    private final TestFrameParser testFrameParser;

    private final Consumer<ProtonBuffer> frameConsumer;

    private AssertionError failureCause;

    private int lastInitiatedChannel = -1;
    private long lastInitiatedLinkHandle = -1;
    private long lastInitiatedCoordinatorLinkHandle = -1;
    private int advertisedIdleTimeout = 0;
    private volatile int emptyFrameCount;

    /**
     * Create a test driver instance connected to the given Engine instance.
     *
     * @param frameConsumer
     *      A {@link Consumer} that will accept encoded frames in ProtonBuffer instances.
     */
    public EngineTestDriver(Consumer<ProtonBuffer> frameConsumer) {
        this.frameConsumer = frameConsumer;

        // Configure test driver resources
        this.testFrameParser = new TestFrameParser(this);
    }

    //----- View the test driver state

    public int getAdvertisedIdleTimeout() {
        return advertisedIdleTimeout;
    }

    public void setAdvertisedIdleTimeout(int advertisedIdleTimeout) {
        this.advertisedIdleTimeout = advertisedIdleTimeout;
    }

    public int getEmptyFrameCount() {
        return emptyFrameCount;
    }

    /**
     * @return the maximum allowed inbound frame size.
     */
    public int getMaxInboundFrameSize() {
        return Integer.MAX_VALUE;
    }

    //----- Test driver handling of decoded AMQP frames

    void handleHeader(AMQPHeader header) {

    }

    void handleSaslPerformative(SaslPerformative sasl, int channel, ProtonBuffer payload) {

    }

    void handlePerformative(Performative amqp, int channel, ProtonBuffer payload) {

    }

    void handleHeartbeat() {

    }

    //----- Test driver actions

    /**
     * @param error
     *      The error that describes why the assertion failed.
     */
    public void assertionFailed(AssertionError error) {
        if (failureCause != null) {
            failureCause = error;
        }
    }

    /**
     * Encodes the given frame data into a ProtonBuffer and injects it into the configured consumer.
     *
     * @param type
     * @param channel
     * @param frameDescribedType
     * @param framePayload
     */
    public void sendFrame(FrameType type, int channel, ListDescribedType frameDescribedType, Binary framePayload) {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        try {
            frameConsumer.accept(buffer);
        } catch (Throwable t) {
            assertionFailed(new AssertionError("Frame was not consumed due to error.", t));
        }
    }

    /**
     * Send the specific header bytes to the remote frame consumer.

     * @param response
     *      The byte array to send as the AMQP Header.
     */
    public void sendHeader(byte[] response) {
        try {
            frameConsumer.accept(ProtonByteBufferAllocator.DEFAULT.wrap(response));
        } catch (Throwable t) {
            assertionFailed(new AssertionError("Frame was not consumed due to error.", t));
        }
    }

    /**
     * @param ex
     */
    public void signalFailureToEngine(IOException ex) {
        // TODO Auto-generated method stub

    }
}
