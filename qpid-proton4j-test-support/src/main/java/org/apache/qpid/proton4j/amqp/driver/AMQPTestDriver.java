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
package org.apache.qpid.proton4j.amqp.driver;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.PerformativeDescribedType;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;

/**
 * Test driver object used to drive inputs and inspect outputs of an Engine.
 */
public class AMQPTestDriver implements Consumer<ProtonBuffer> {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(AMQPTestDriver.class);

    private final FrameDecoder frameParser;
    private final FrameEncoder frameEncoder;

    private final Consumer<ProtonBuffer> frameConsumer;

    private AssertionError failureCause;

    private int advertisedIdleTimeout = 0;
    private volatile int emptyFrameCount;

    private int inboundMaxFrameSize = Integer.MAX_VALUE;
    private int outboundMaxFrameSize = Integer.MAX_VALUE;

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
    public AMQPTestDriver(Consumer<ProtonBuffer> frameConsumer) {
        this.frameConsumer = frameConsumer;

        // Configure test driver resources
        this.frameParser = new FrameDecoder(this);
        this.frameEncoder = new FrameEncoder(this);
    }

    /**
     * Creates and returns a new script writer for use in building the expectations
     * and actions that will comprise an AMQP test.
     *
     * @return a new {@link ScriptWriter} linked to this driver.
     */
    public ScriptWriter createScriptWriter() {
        return new ScriptWriter(this);
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
    public int getInboundMaxFrameSize() {
        return inboundMaxFrameSize;
    }

    public void setInboundMaxFrameSize(int maxSize) {
        this.inboundMaxFrameSize = maxSize;
    }

    /**
     * @return the maximum allowed outbound frame size.
     */
    public int getOutboundMaxFrameSize() {
        return outboundMaxFrameSize;
    }

    public void setOutboundMaxFrameSize(int maxSize) {
        this.outboundMaxFrameSize = maxSize;
    }

    //----- Accepts encoded AMQP frames for processing

    @Override
    public void accept(ProtonBuffer buffer) {
        LOG.trace("Driver processing new inbound buffer of size: {}", buffer.getReadableBytes());

        try {
            // Process off all encoded frames from this buffer one at a time.
            while (buffer.isReadable() && failureCause == null) {
                LOG.trace("Driver ingesting {} bytes.", buffer.getReadableBytes());
                frameParser.ingest(buffer);
                LOG.trace("Driver ingestion completed cycle, remaining bytes in buffer: {}", buffer.getReadableBytes());
            }
        } catch (AssertionError e) {
            signalFailure(e);
        }
    }

    //----- Test driver handling of decoded AMQP frames

    void handleHeader(AMQPHeader header) throws AssertionError {
        ScriptedElement scriptEntry = script.poll();
        if (scriptEntry == null) {
            signalFailure(new AssertionError("Received header when not expecting any input."));
        }

        header.invoke(scriptEntry, this);
        prcessScript(scriptEntry);
    }

    void handleSaslPerformative(SaslDescribedType sasl, int channel, ProtonBuffer payload) throws AssertionError {
        ScriptedElement scriptEntry = script.poll();
        if (scriptEntry == null) {
            signalFailure(new AssertionError("Received performative[" + sasl + "] when not expecting any input."));
        }

        sasl.invoke(scriptEntry, this);
        prcessScript(scriptEntry);
    }

    void handlePerformative(PerformativeDescribedType amqp, int channel, ProtonBuffer payload) throws AssertionError {
        ScriptedElement scriptEntry = script.poll();
        if (scriptEntry == null) {
            signalFailure(new AssertionError("Received performative[" + amqp + "] when not expecting any input."));
        }

        amqp.invoke(scriptEntry, payload, channel, this);
        prcessScript(scriptEntry);
    }

    void handleHeartbeat() {
        emptyFrameCount++;
    }

    //----- Test driver actions

    /**
     * Checks that the test script was fully consumed and that the driver is not
     * in an error state.
     *
     * @throws AssertionError if the scripted events are not all completed when called.
     */
    public void assertScriptComplete() throws AssertionError {
        checkFailed();
        if (!script.isEmpty()) {
            // TODO - Dump all elements that were not executed in the script.
            throw new AssertionError("Not all expected actions were completed");
        }
    }

    /**
     * Checks that the test script was fully consumed but ignores any failure
     * state as that may have been expected.
     *
     * @throws AssertionError if the scripted events are not all completed when called.
     */
    public void assertScriptCompleteIngoreErrors() throws AssertionError {
        if (!script.isEmpty()) {
            // TODO - Dump all elements that were not executed in the script.
            throw new AssertionError("Not all expected actions were completed");
        }
    }

    public void addScriptedElement(ScriptedElement element) {
        checkFailed();
        script.offer(element);
    }

    public void performImmediately(ScriptedAction action) {
        checkFailed();
        action.perform(this);
        while (action.performAfterwards() != null && failureCause == null) {
            action.performAfterwards().perform(this);
        }
    }

    /**
     * Encodes the given frame data into a ProtonBuffer and injects it into the configured consumer.
     *
     * @param channel
     * @param performative
     * @param payload
     */
    public void sendAMQPFrame(int channel, DescribedType performative, ProtonBuffer payload) {
        // TODO - handle split frames when frame size requires it
        ProtonBuffer buffer = frameEncoder.handleWrite(performative, channel, payload, null);

        try {
            frameConsumer.accept(buffer);
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not consumed due to error.", t));
        }
    }

    /**
     * Encodes the given frame data into a ProtonBuffer and injects it into the configured consumer.
     *
     * @param channel
     * @param performative
     */
    public void sendSaslFrame(int channel, DescribedType performative) {
        ProtonBuffer buffer = frameEncoder.handleWrite(performative, channel);

        try {
            frameConsumer.accept(buffer);
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not consumed due to error.", t));
        }
    }

    /**
     * Send the specific header bytes to the remote frame consumer.

     * @param header
     *      The byte array to send as the AMQP Header.
     */
    public void sendHeader(AMQPHeader header) {
        try {
            frameConsumer.accept(header.getBuffer());
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not consumed due to error.", t));
        }
    }

    /**
     * Throw an exception from processing incoming data which should be handled by the peer under test.
     *
     * @param ex
     *      The exception that triggered this call.
     *
     * @throws AssertionError indicating the first error that cause the driver to report test failure.
     */
    public void signalFailure(Throwable ex) throws AssertionError {
        if (this.failureCause == null) {
            if (ex instanceof AssertionError) {
                this.failureCause = (AssertionError) ex;
            } else {
                this.failureCause = new AssertionError(ex);
            }
        }

        throw failureCause;
    }

    /**
     * Throw an exception from processing incoming data which should be handled by the peer under test.
     *
     * @param message
     *      The error message that describes what triggered this call.
     *
     * @throws AssertionError that indicates the first error that failed for this driver.
     */
    public void signalFailure(String message) throws AssertionError {
        signalFailure(new AssertionError(message));
    }

    //----- Internal implementation

    private void prcessScript(ScriptedElement current) {
        while (current.performAfterwards() != null && failureCause == null) {
            current.performAfterwards().perform(this);
        }

        ScriptedElement peekNext = script.peek();
        do {
            if (peekNext instanceof ScriptedAction) {
                script.poll();
                ((ScriptedAction) peekNext).perform(this);
            } else {
                return;
            }

            peekNext = script.peek();
        } while (peekNext != null && failureCause == null);
    }

    private void checkFailed() {
        if (failureCause != null) {
            throw failureCause;
        }
    }
}
