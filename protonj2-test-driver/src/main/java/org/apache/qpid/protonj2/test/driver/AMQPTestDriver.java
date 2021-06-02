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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.qpid.protonj2.test.driver.actions.ScriptCompleteAction;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslOutcome;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.codec.transport.HeartBeat;
import org.apache.qpid.protonj2.test.driver.codec.transport.Open;
import org.apache.qpid.protonj2.test.driver.codec.transport.PerformativeDescribedType;
import org.apache.qpid.protonj2.test.driver.exceptions.UnexpectedPerformativeError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Test driver object used to drive inputs and inspect outputs of an Engine.
 */
public class AMQPTestDriver implements Consumer<ByteBuffer> {

    private static final Logger LOG = LoggerFactory.getLogger(AMQPTestDriver.class);

    private final String driverName;
    private final FrameDecoder frameParser;
    private final FrameEncoder frameEncoder;

    private Open localOpen;
    private Open remoteOpen;

    private final DriverSessions sessions = new DriverSessions(this);

    private final Consumer<ByteBuffer> frameConsumer;
    private final Consumer<AssertionError> assertionConsumer;
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
     * @param name
     *      A unique test driver name that should allow log inspection of sends and receiver for a driver instance.
     * @param frameConsumer
     *      A {@link Consumer} that will accept encoded frames in ProtonBuffer instances.
     * @param scheduler
     *      A {@link Supplier} that will provide this driver with a scheduler service for delayed actions
     */
    public AMQPTestDriver(String name, Consumer<ByteBuffer> frameConsumer, Supplier<ScheduledExecutorService> scheduler) {
        this(name, frameConsumer, null, scheduler);
    }

    /**
     * Create a test driver instance connected to the given Engine instance.
     *
     * @param name
     *      A unique test driver name that should allow log inspection of sends and receiver for a driver instance.
     * @param frameConsumer
     *      A {@link Consumer} that will accept encoded frames in ProtonBuffer instances.
     * @param assertionConsumer
     *      A {@link Consumer} that will handle test assertions from the scripted expectations
     * @param scheduler
     *      A {@link Supplier} that will provide this driver with a scheduler service for delayed actions
     */
    public AMQPTestDriver(String name, Consumer<ByteBuffer> frameConsumer, Consumer<AssertionError> assertionConsumer, Supplier<ScheduledExecutorService> scheduler) {
        this.frameConsumer = frameConsumer;
        this.assertionConsumer = assertionConsumer;
        this.schedulerSupplier = scheduler;
        this.driverName = name;

        // Configure test driver resources
        this.frameParser = new FrameDecoder(this);
        this.frameEncoder = new FrameEncoder(this);
    }

    /**
     * @return the Sessions tracking manager for this driver.
     */
    public DriverSessions sessions() {
        return sessions;
    }

    /**
     * @return the assigned name of this AMQP test driver
     */
    public Object getName() {
        return driverName;
    }

    //----- View the test driver state

    /**
     * @return the idle timeout value that will be set on outgoing Open frames.
     */
    public int getAdvertisedIdleTimeout() {
        return advertisedIdleTimeout;
    }

    /**
     * Sets the idle timeout which the test driver should supply to remote in its Open frame.
     *
     * @param advertisedIdleTimeout
     * 		The idle timeout value to supply to remote peers.
     */
    public void setAdvertisedIdleTimeout(int advertisedIdleTimeout) {
        this.advertisedIdleTimeout = advertisedIdleTimeout;
    }

    /**
     * @return the number of empty frames that the remote has sent for idle timeout processing.
     */
    public int getEmptyFrameCount() {
        return emptyFrameCount;
    }

    /**
     * @return the number of performative frames that this test peer has processed from incoming bytes.
     */
    public int getPerformativeCount() {
        return performativeCount;
    }

    /**
     * @return the number of SASL performative frames that this test peer has processed from incoming bytes.
     */
    public int getSaslPerformativeCount() {
        return saslPerformativeCount;
    }

    /**
     * @return the maximum allowed inbound frame size.
     */
    public int getInboundMaxFrameSize() {
        return inboundMaxFrameSize;
    }

    /**
     * Sets a limiting value on the size of incoming frames after which the peer will signal an error.
     *
     * @param maxSize
     * 		The maximum incoming frame size limit for this peer.
     */
    public void setInboundMaxFrameSize(int maxSize) {
        this.inboundMaxFrameSize = maxSize;
    }

    /**
     * @return the maximum allowed outbound frame size.
     */
    public int getOutboundMaxFrameSize() {
        return outboundMaxFrameSize;
    }

    /**
     * Sets a limiting value on the size of outgoing frames after which the peer will signal an error.
     *
     * @param maxSize
     * 		The maximum outgoing frame size limit for this peer.
     */
    public void setOutboundMaxFrameSize(int maxSize) {
        this.outboundMaxFrameSize = maxSize;
    }

    //----- Accepts encoded AMQP frames for processing

    @Override
    public void accept(ByteBuffer buffer) {
        accept(Unpooled.wrappedBuffer(buffer));
    }

    /**
     * Supply incoming bytes read to the test driver via an Netty {@link ByteBuf} instance.
     *
     * @param buffer
     * 		The Netty {@link ByteBuf} that contains new incoming bytes read.
     */
    public void accept(ByteBuf buffer) {
        LOG.trace("{} processing new inbound buffer of size: {}", driverName, buffer.readableBytes());

        try {
            // Process off all encoded frames from this buffer one at a time.
            while (buffer.isReadable() && failureCause == null) {
                LOG.trace("{} ingesting {} bytes.", driverName, buffer.readableBytes());
                frameParser.ingest(buffer);
                LOG.trace("{} ingestion completed cycle, remaining bytes in buffer: {}", driverName, buffer.readableBytes());
            }
        } catch (AssertionError e) {
            signalFailure(e);
        }
    }

    /**
     * @return the remote {@link Open} that this peer received (if any).
     */
    public Open getRemoteOpen() {
        return remoteOpen;
    }

    /**
     * @return the local {@link Open} that this peer sent (if any).
     */
    public Open getLocalOpen() {
        return localOpen;
    }

    //----- Test driver handling of decoded AMQP frames

    void handleConnectedEstablished() throws AssertionError {
        synchronized (script) {
            ScriptedElement peekNext = script.peek();
            if (peekNext instanceof ScriptedAction) {
                prcessScript(peekNext);
            }
        }
    }

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
                    signalFailure(t);
                    throw t;
                }
            }

            prcessScript(scriptEntry);
        }
    }

    void handleSaslPerformative(int frameSize, SaslDescribedType sasl, int channel, ByteBuf payload) throws AssertionError {
        synchronized (script) {
            final ScriptedElement scriptEntry = script.poll();
            if (scriptEntry == null) {
                signalFailure(new AssertionError("Received performative[" + sasl + "] when not expecting any input."));
            }

            try {
                sasl.invoke(scriptEntry, frameSize, this);
            } catch (UnexpectedPerformativeError e) {
                if (scriptEntry.isOptional()) {
                    handleSaslPerformative(frameSize, sasl, channel, payload);
                } else {
                    signalFailure(e);
                    throw e;
                }
            } catch (AssertionError assertion) {
                LOG.warn(assertion.getMessage());
                signalFailure(assertion);
                throw assertion;
            }

            prcessScript(scriptEntry);
        }
    }

    void handlePerformative(int frameSize, PerformativeDescribedType amqp, int channel, ByteBuf payload) throws AssertionError {
        switch (amqp.getPerformativeType()) {
            case HEARTBEAT:
                break;
            case OPEN:
                remoteOpen = (Open) amqp;
            default:
                performativeCount++;
                break;
        }

        synchronized (script) {
            final ScriptedElement scriptEntry = script.poll();
            if (scriptEntry == null) {
                signalFailure(new AssertionError("Received performative[" + amqp + "] when not expecting any input."));
            }

            try {
                amqp.invoke(scriptEntry, frameSize, payload, channel, this);
            } catch (UnexpectedPerformativeError e) {
                if (scriptEntry.isOptional()) {
                    handlePerformative(frameSize, amqp, channel, payload);
                } else {
                    signalFailure(e);
                    throw e;
                }
            } catch (AssertionError assertion) {
                LOG.warn(assertion.getMessage());
                signalFailure(assertion);
                throw assertion;
            }

            prcessScript(scriptEntry);
        }
    }

    void handleHeartbeat(int frameSize, int channel) {
        emptyFrameCount++;
        handlePerformative(frameSize, HeartBeat.INSTANCE, channel, null);
    }

    /**
     * Submits a new {@link ScriptedAction} which should be performed after waiting for the
     * given delay period in milliseconds.
     *
     * @param delay
     * 		The time in milliseconds to wait before performing the action
     * @param action
     *      The action that should be performed after the given delay.
     */
    public synchronized void afterDelay(int delay, ScriptedAction action) {
        Objects.requireNonNull(schedulerSupplier, "This driver cannot schedule delayed events, no scheduler available");
        ScheduledExecutorService scheduler = schedulerSupplier.get();
        Objects.requireNonNull(scheduler, "This driver cannot schedule delayed events, no scheduler available");

        scheduler.schedule(() -> {
            LOG.trace("{} running delayed action: {}", driverName, action);
            action.perform(this);
        }, delay, TimeUnit.MILLISECONDS);
    }

    //----- Test driver actions

    /**
     * Waits indefinitely for the scripted expectations and actions to be performed.  If the script
     * execution encounters an error this method will throw an {@link AssertionError} that describes
     * the error.
     */
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

        checkFailed();
    }

    /**
     * Waits indefinitely for the scripted expectations and actions to be performed.  If the script
     * execution encounters an error this method will not throw an {@link AssertionError} that describes
     * the error but simply ignore it and return.
     */
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

    /**
     * Waits for the given amount of time for the scripted expectations and actions to be performed.  If
     * the script execution encounters an error this method will throw an {@link AssertionError} that
     * describes the error.
     *
     * @param timeout
     * 		The time in milliseconds to wait for the scripted expectations to complete.
     */
    public void waitForScriptToComplete(long timeout) {
        waitForScriptToComplete(timeout, TimeUnit.SECONDS);
    }

    /**
     * Waits for the given amount of time for the scripted expectations and actions to be performed.  If
     * the script execution encounters an error this method will throw an {@link AssertionError} that
     * describes the error.
     *
     * @param timeout
     * 		The time to wait for the scripted expectations to complete.
     * @param units
     * 		The {@link TimeUnit} instance that converts the given time value.
     */
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

        checkFailed();
    }

    /**
     * Adds the script element to the list of scripted expectations and actions that should occur
     * in order to complete the test outcome.
     *
     * @param element
     * 		The element to add to the script.
     */
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
    public void sendAMQPFrame(int channel, DescribedType performative, ByteBuf payload) {
        LOG.trace("{} Sending performative: {}", driverName, performative);

        if (performative instanceof PerformativeDescribedType) {
            switch (((PerformativeDescribedType) performative).getPerformativeType()) {
                case OPEN:
                    localOpen = (Open) performative;
                default:
                    break;
            }
        }

        try {
            final ByteBuf buffer = frameEncoder.handleWrite(performative, channel, payload, null);
            LOG.trace("{} Writing out buffer {} to consumer: {}", driverName, buffer, frameConsumer);
            frameConsumer.accept(buffer.nioBuffer());
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

        LOG.trace("{} Sending sasl performative: {}", driverName, performative);

        try {
            final ByteBuf buffer = frameEncoder.handleWrite(performative, channel);
            frameConsumer.accept(buffer.nioBuffer());
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
        LOG.trace("{} Sending AMQP Header: {}", driverName, header);
        try {
            frameConsumer.accept(ByteBuffer.wrap(header.getBuffer()));
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
        ByteBuf buffer = frameEncoder.handleWrite(null, channel, null, null);

        try {
            frameConsumer.accept(buffer.nioBuffer());
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not consumed due to error.", t));
        }
    }

    /**
     * Send the specific ProtonBuffer bytes to the remote frame consumer.

     * @param buffer
     *      The buffer whose contents are to be written to the frame consumer.
     */
    public void sendBytes(ByteBuffer buffer) {
        LOG.trace("{} Sending bytes from ByteBuffer: {}", driverName, buffer);
        try {
            frameConsumer.accept(buffer.duplicate());
        } catch (Throwable t) {
            signalFailure(new AssertionError("Buffer was not consumed due to error.", t));
        }
    }

    /**
     * Send the specific ProtonBuffer bytes to the remote frame consumer.

     * @param buffer
     *      The buffer whose contents are to be written to the frame consumer.
     */
    public void sendBytes(ByteBuf buffer) {
        LOG.trace("{} Sending bytes from ProtonBuffer: {}", driverName, buffer);
        try {
            frameConsumer.accept(buffer.nioBuffer());
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
                LOG.trace("{} sending failure assertion due to: ", driverName, ex);
                this.failureCause = (AssertionError) ex;
            } else {
                LOG.trace("{} sending failure assertion due to: ", driverName, ex);
                this.failureCause = new AssertionError(ex);
            }

            searchForScriptioCompletionAndTrigger();

            if (assertionConsumer != null) {
                assertionConsumer.accept(failureCause);
            }
        }
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
