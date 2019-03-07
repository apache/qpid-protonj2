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
import java.util.ArrayDeque;
import java.util.Queue;
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
import org.apache.qpid.proton4j.engine.test.peer.FrameType;
import org.apache.qpid.proton4j.engine.test.peer.ListDescribedType;

/**
 * Test driver object used to drive inputs and inspect outputs of an Engine.
 */
public class EngineTestDriver implements Consumer<ProtonBuffer> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(EngineTestDriver.class);

    private static final Symbol ANONYMOUS = Symbol.valueOf("ANONYMOUS");
    private static final Symbol EXTERNAL = Symbol.valueOf("EXTERNAL");
    private static final Symbol PLAIN = Symbol.valueOf("PLAIN");

    private final FrameParser frameParser;

    private final Consumer<ProtonBuffer> frameConsumer;

    private IOException failureCause;

    private int lastInitiatedChannel = -1;
    private long lastInitiatedLinkHandle = -1;
    private long lastInitiatedCoordinatorLinkHandle = -1;
    private int advertisedIdleTimeout = 0;
    private volatile int emptyFrameCount;

    /**
     *  Holds the expectations for processing of data from the peer under test.
     */
    private final Queue<ScriptedElement> script = new ArrayDeque<>();

    /**
     * Create a test driver instance connected to the given Engine instance.
     *
     * @param frameConsumer
     *      A {@link Consumer} that will accept encoded frames in ProtonBuffer instances.
     */
    public EngineTestDriver(Consumer<ProtonBuffer> frameConsumer) {
        this.frameConsumer = frameConsumer;

        // Configure test driver resources
        this.frameParser = new FrameParser(this);
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

    //----- Accepts encoded AMQP frames for processing

    @Override
    public void accept(ProtonBuffer buffer) {
        try {
            // Process off all encoded frames from this buffer one at a time.
            while (buffer.isReadable() && failureCause == null) {
                frameParser.ingest(buffer);
            }
        } catch (IOException e) {
            try {
                signalFailure(e);
            } catch (IOException e1) {
                // TODO - send error via a RuntimeException
            }
        }
    }

    //----- Test driver handling of decoded AMQP frames

    void handleHeader(AMQPHeader header) throws IOException {
        ScriptedElement scriptEntry = script.poll();
        if (scriptEntry == null) {
            signalFailure(new IOException("Receibed header when not expecting any input."));
        }

        header.invoke(scriptEntry, this);
        while (scriptEntry.performAfterwards() != null) {
            scriptEntry.performAfterwards().perform(this, frameConsumer);
        }
    }

    void handleSaslPerformative(SaslPerformative sasl, int channel, ProtonBuffer payload) throws IOException {
        ScriptedElement scriptEntry = script.poll();
        if (scriptEntry == null) {
            signalFailure(new IOException("Receibed header when not expecting any input."));
        }

        sasl.invoke(scriptEntry, this);
        while (scriptEntry.performAfterwards() != null) {
            scriptEntry.performAfterwards().perform(this, frameConsumer);
        }
    }

    void handlePerformative(Performative amqp, int channel, ProtonBuffer payload) throws IOException {
        ScriptedElement scriptEntry = script.poll();
        if (scriptEntry == null) {
            signalFailure(new IOException("Receibed header when not expecting any input."));
        }

        amqp.invoke(scriptEntry, payload, channel, this);
        while (scriptEntry.performAfterwards() != null) {
            scriptEntry.performAfterwards().perform(this, frameConsumer);
        }
    }

    void handleHeartbeat() {
        emptyFrameCount++;
    }

    //----- Test driver actions

    /**
     * @param error
     *      The error that describes why the assertion failed.
     */
    public void assertionFailed(AssertionError error) {
        if (failureCause != null) {
            failureCause = new IOException("Assertion caused failure in driver.", error);
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
     * Throw an exception from processing incoming data which should be handled by the peer under test.
     *
     * @param ex
     *      The exception that triggered this call.
     *
     * @throws IOException that indicates the first error that failed for this driver.
     */
    public void signalFailure(IOException ex) throws IOException {
        throw ex; // TODO
    }

    /**
     * Throw an exception from processing incoming data which should be handled by the peer under test.
     *
     * @param message
     *      The error message that describes what triggered this call.
     *
     * @throws IOException that indicates the first error that failed for this driver.
     */
    public void signalFailure(String message) throws IOException {
        throw new IOException(message);
    }

    //----- Expectations

    public void expectHeader(AMQPHeader header) {
        expectHeader(header, null);
    }

    public void expectHeader(byte[] headerBytes) {
        expectHeader(headerBytes, null);
    }

    public void expectHeader(AMQPHeader header, AMQPHeader response) {
        // TODO
    }

    public void expectHeader(byte[] headerBytes, byte[] responseBytes) {
        expectHeader(new AMQPHeader(headerBytes), new AMQPHeader(responseBytes));
    }
}
