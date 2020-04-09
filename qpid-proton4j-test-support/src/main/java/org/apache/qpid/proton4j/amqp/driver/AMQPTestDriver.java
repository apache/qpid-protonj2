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
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.driver.actions.ScriptCompleteAction;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.HeartBeat;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.PerformativeDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.PerformativeDescribedType.PerformativeType;
import org.apache.qpid.proton4j.amqp.driver.exceptions.UnexpectedPerformativeError;
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

    private final DriverSessions sessions = new DriverSessions(this);

    private final Consumer<ProtonBuffer> frameConsumer;
    private final Supplier<ScheduledExecutorService> schedulerSupplier;

    private volatile AssertionError failureCause;

    private int advertisedIdleTimeout = 0;

    private volatile int emptyFrameCount;
    private volatile int performativeCount;
    private volatile int saslPerformativeCount;

    private int inboundMaxFrameSize = Integer.MAX_VALUE;
    private int outboundMaxFrameSize = Integer.MAX_VALUE;

    /**
     *  Holds the expectations for processing of data from the peer under test.
     *  Uses a thread safe queue to avoid contention on adding script entries
     *  and processing incoming data (although you should probably not do that).
     */
    private final Queue<ScriptedElement> script = new ArrayDeque<>();

    /**
     * Create a test driver instance connected to the given Engine instance.
     *
     * @param frameConsumer
     *      A {@link Consumer} that will accept encoded frames in ProtonBuffer instances.
     * @param scheduler
     *      A {@link Supplier} that will provide this driver with a scheduler service for delayed actions
     */
    public AMQPTestDriver(Consumer<ProtonBuffer> frameConsumer, Supplier<ScheduledExecutorService> scheduler) {
        this.frameConsumer = frameConsumer;
        this.schedulerSupplier = scheduler;

        // Configure test driver resources
        this.frameParser = new FrameDecoder(this);
        this.frameEncoder = new FrameEncoder(this);
    }

    /**
     * @return the Sessions tracking manager for this driver.
     */
    public DriverSessions getSessions() {
        return sessions;
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

    public int getPerformativeCount() {
        return performativeCount;
    }

    public int getSaslPerformativeCount() {
        return saslPerformativeCount;
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
        synchronized (script) {
            final ScriptedElement scriptEntry = script.poll();

            if (scriptEntry == null) {
                signalFailure(new AssertionError("Received header when not expecting any input."));
            }

            try {
                header.invoke(scriptEntry, this);
            } catch (Throwable t) {
                if (scriptEntry.isOptional()) {
                    handleHeader(header);
                } else {
                    LOG.warn(t.getMessage());
                    searchForScriptioCompletionAndTrigger();
                    throw t;
                }
            }

            prcessScript(scriptEntry);
        }
    }

    void handleSaslPerformative(SaslDescribedType sasl, int channel, ProtonBuffer payload) throws AssertionError {
        synchronized (script) {
            final ScriptedElement scriptEntry = script.poll();
            if (scriptEntry == null) {
                signalFailure(new AssertionError("Received performative[" + sasl + "] when not expecting any input."));
            }

            try {
                sasl.invoke(scriptEntry, this);
            } catch (UnexpectedPerformativeError e) {
                if (scriptEntry.isOptional()) {
                    handleSaslPerformative(sasl, channel, payload);
                } else {
                    throw e;
                }
            } catch (AssertionError assertion) {
                LOG.warn(assertion.getMessage());
                searchForScriptioCompletionAndTrigger();
                throw assertion;
            }

            prcessScript(scriptEntry);
        }
    }

    void handlePerformative(PerformativeDescribedType amqp, int channel, ProtonBuffer payload) throws AssertionError {
        if (!amqp.getPerformativeType().equals(PerformativeType.HEARTBEAT)) {
            performativeCount++;
        }

        synchronized (script) {
            final ScriptedElement scriptEntry = script.poll();
            if (scriptEntry == null) {
                // TODO - Need to ensure a readable error by converting the codec type to a true performative type when
                //        logging what happened here.
                signalFailure(new AssertionError("Received performative[" + amqp + "] when not expecting any input."));
            }

            try {
                amqp.invoke(scriptEntry, payload, channel, this);
            } catch (UnexpectedPerformativeError e) {
                if (scriptEntry.isOptional()) {
                    handlePerformative(amqp, channel, payload);
                } else {
                    throw e;
                }
            } catch (AssertionError assertion) {
                LOG.warn(assertion.getMessage());
                searchForScriptioCompletionAndTrigger();
                throw assertion;
            }

            prcessScript(scriptEntry);
        }
    }

    void handleHeartbeat(int channel) {
        emptyFrameCount++;
        handlePerformative(HeartBeat.INSTANCE, channel, null);
    }

    public synchronized void afterDelay(int delay, ScriptedAction action) {
        Objects.requireNonNull(schedulerSupplier, "This driver cannot schedule delayed events, no scheduler available");
        ScheduledExecutorService scheduler = schedulerSupplier.get();
        Objects.requireNonNull(scheduler, "This driver cannot schedule delayed events, no scheduler available");

        scheduler.schedule(() -> {
            LOG.trace("AMQPTestPeer running delayed action: {}", action);
            action.perform(this);
        }, delay, TimeUnit.MILLISECONDS);
    }

    //----- Test driver actions

    public void waitForScriptToComplete() {
        checkFailed();

        ScriptCompleteAction possibleWait = null;

        synchronized (script) {
            checkFailed();
            if (!script.isEmpty()) {
                possibleWait = new ScriptCompleteAction(this).queue();
            }
        }

        if (possibleWait != null) {
            try {
                possibleWait.await();
            } catch (InterruptedException e) {
                Thread.interrupted();
                signalFailure("Interrupted while waiting for script to complete");
            }
        }
    }

    public void waitForScriptToCompleteIgnoreErrors() {
        ScriptCompleteAction possibleWait = null;

        synchronized (script) {
            if (!script.isEmpty()) {
                possibleWait = new ScriptCompleteAction(this).queue();
            }
        }

        if (possibleWait != null) {
            try {
                possibleWait.await();
            } catch (InterruptedException e) {
                Thread.interrupted();
                signalFailure("Interrupted while waiting for script to complete");
            }
        }
    }

    public void waitForScriptToComplete(long timeout) {
        waitForScriptToComplete(timeout, TimeUnit.SECONDS);
    }

    public void waitForScriptToComplete(long timeout, TimeUnit units) {
        checkFailed();

        ScriptCompleteAction possibleWait = null;

        synchronized (script) {
            checkFailed();
            if (!script.isEmpty()) {
                possibleWait = new ScriptCompleteAction(this).queue();
            }
        }

        if (possibleWait != null) {
            try {
                possibleWait.await(timeout, units);
            } catch (InterruptedException e) {
                Thread.interrupted();
                signalFailure("Interrupted while waiting for script to complete");
            }
        }
    }

    public void addScriptedElement(ScriptedElement element) {
        checkFailed();
        synchronized (script) {
            checkFailed();
            script.offer(element);
        }
    }

    /**
     * Encodes the given frame data into a ProtonBuffer and injects it into the configured consumer.
     *
     * @param channel
     *      The channel to use when writing the frame
     * @param performative
     *      The AMQP Performative to write
     * @param payload
     *      The payload to include in the encoded frame.
     */
    public void sendAMQPFrame(int channel, DescribedType performative, ProtonBuffer payload) {
        LOG.trace("Sending performative: {}", performative);
        // TODO - handle split frames when frame size requires it

        try {
            final ProtonBuffer buffer = frameEncoder.handleWrite(performative, channel, payload, null);
            LOG.trace("Writing out buffer {} to consumer: {}", buffer, frameConsumer);
            frameConsumer.accept(buffer);
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not written due to error.", t));
        }
    }

    /**
     * Encodes the given frame data into a ProtonBuffer and injects it into the configured consumer.
     *
     * @param channel
     *      The channel to use when writing the frame
     * @param performative
     *      The SASL Performative to write
     */
    public void sendSaslFrame(int channel, DescribedType performative) {
        // When the outcome of SASL is written the decoder should revert to initial state
        // as the only valid next incoming value is an AMQP header.
        if (performative instanceof SaslOutcome) {
            frameParser.resetToExpectingHeader();
        }

        LOG.trace("Sending sasl performative: {}", performative);

        try {
            final ProtonBuffer buffer = frameEncoder.handleWrite(performative, channel);
            frameConsumer.accept(buffer);
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not written due to error.", t));
        }
    }

    /**
     * Send the specific header bytes to the remote frame consumer.

     * @param header
     *      The byte array to send as the AMQP Header.
     */
    public void sendHeader(AMQPHeader header) {
        LOG.trace("Sending AMQP Header: {}", header);
        try {
            frameConsumer.accept(header.getBuffer());
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not consumed due to error.", t));
        }
    }

    /**
     * Send an Empty Frame on the given channel to the remote consumer.
     *
     * @param channel
     *      the channel on which to send the empty frame.
     */
    public void sendEmptyFrame(int channel) {
        ProtonBuffer buffer = frameEncoder.handleWrite(null, channel, null, null);

        try {
            frameConsumer.accept(buffer);
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not consumed due to error.", t));
        }
    }

    /**
     * Send the specific ProtonBuffer bytes to the remote frame consumer.

     * @param buffer
     *      The buffer whose contents are to be written to the frame consumer.
     */
    public void sendBytes(ProtonBuffer buffer) {
        LOG.trace("Sending bytes from ProtonBuffer: {}", buffer);
        try {
            frameConsumer.accept(buffer.duplicate());
        } catch (Throwable t) {
            signalFailure(new AssertionError("Buffer was not consumed due to error.", t));
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
                LOG.trace("Test Driver sending failure assertion due to: ", ex.getCause());
                this.failureCause = (AssertionError) ex;
            } else {
                LOG.trace("Test Driver sending failure assertion due to: ", ex);
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

    private void searchForScriptioCompletionAndTrigger() {
        script.forEach(element -> {
            if (element instanceof ScriptCompleteAction) {
                ScriptCompleteAction completed = (ScriptCompleteAction) element;
                completed.perform(this);
            }
        });
    }

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
