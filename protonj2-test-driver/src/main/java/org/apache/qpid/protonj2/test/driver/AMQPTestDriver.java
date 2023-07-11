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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
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
import org.apache.qpid.protonj2.test.driver.expectations.ConnectionDropExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.SaslOutcomeExpectation;
import org.apache.qpid.protonj2.test.driver.netty.NettyEventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Supplier<NettyEventLoop> schedulerSupplier;

    private volatile List<ByteBuffer> deferredWrites = new ArrayList<>();
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
    public AMQPTestDriver(String name, Consumer<ByteBuffer> frameConsumer, Supplier<NettyEventLoop> scheduler) {
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
    public AMQPTestDriver(String name, Consumer<ByteBuffer> frameConsumer, Consumer<AssertionError> assertionConsumer, Supplier<NettyEventLoop> scheduler) {
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

    /**
     * Supply incoming bytes read to the test driver via an Netty {@link ByteBuffer} instance.
     *
     * @param buffer
     * 		The Netty {@link ByteBuffer} that contains new incoming bytes read.
     */
    @Override
    public void accept(ByteBuffer buffer) {
        LOG.trace("{} processing new inbound buffer of size: {}", driverName, buffer.remaining());

        try {
            // Process off all encoded frames from this buffer one at a time.
            while (buffer.remaining() > 0 && failureCause == null) {
                LOG.trace("{} ingestion of {} bytes starting now.", driverName, buffer.remaining());
                frameParser.ingest(buffer);
                LOG.trace("{} ingestion completed cycle, remaining bytes in buffer: {}", driverName, buffer.remaining());
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
            // Any new AMQP connection starts with the header so we reset the parser
            // to begin a new exchange.
            ScriptedElement peekNext = script.peek();
            if (peekNext instanceof ScriptedAction) {
                processScript(peekNext);
            }
            resetToExpectingAMQPHeader();
        }
    }

    void handleConnectedDropped() throws AssertionError {
        synchronized (script) {
            // For now we just reset the parser as any new connection would need to
            // send an AMQP header, other validation could be added if we expand
            // processing on client disconnect events.
            resetToExpectingAMQPHeader();
            // Reset connection tracking state to empty as the connection is gone
            // and we want new connection to be able to reuse handles etc.
            sessions.reset();
            // Check if the currently pending scripted expectation is for the connection
            // to drop in which case we remove it and unblock any waiters on the script
            // to complete, if this is the final scripted entry.
            final ScriptedElement scriptEntry = script.peek();
            if (scriptEntry instanceof ConnectionDropExpectation) {
                processScript(script.poll());
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

            processScript(scriptEntry);
        }
    }

    void handleSaslPerformative(int frameSize, SaslDescribedType sasl, int channel, ByteBuffer payload) throws AssertionError {
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

            processScript(scriptEntry);
        }
    }

    void handlePerformative(int frameSize, PerformativeDescribedType amqp, int channel, ByteBuffer payload) throws AssertionError {
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

            processScript(scriptEntry);
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
        NettyEventLoop scheduler = schedulerSupplier.get();
        Objects.requireNonNull(scheduler, "This driver cannot schedule delayed events, no scheduler available");

        scheduler.schedule(() -> {
            LOG.trace("{} running delayed action: {}", driverName, action);
            action.perform(this);
        }, delay, TimeUnit.MILLISECONDS);
    }

    //----- Test driver actions

    /**
     * Resets the frame parser to a state where it expects to read an AMQP Header type instead
     * of a normal AMQP frame type.  By default the parser starts in this state and then switches
     * to frames after the first header is read.  In cases where SASL is in use this needs to be
     * reset to the header read state when the outcome is known.  Normally the script should handle
     * this via a {@link SaslOutcomeExpectation} but other script variations might want to drive
     * this manually.
     */
    public void resetToExpectingAMQPHeader() {
        this.frameParser.resetToExpectingHeader();
    }

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
    public final void sendAMQPFrame(int channel, DescribedType performative, ByteBuffer payload) {
        sendAMQPFrame(channel, performative, payload, false);
    }

    /**
     * Encodes the given frame data into a ProtonBuffer but does not write the data until the next non-deferred
     * send if initiated.
     *
     * @param channel
     *      The channel to use when writing the frame
     * @param performative
     *      The AMQP Performative to write
     * @param payload
     *      The payload to include in the encoded frame.
     * @param splitWrite
     * 		Should the data be written in multiple chunks
     */
    public void deferAMQPFrame(int channel, DescribedType performative, ByteBuffer payload, boolean splitWrite) {
        LOG.trace("{} Deferring write of performative: {}", driverName, performative);
        try {
            deferredWrites.add(frameEncoder.handleWrite(performative, channel, payload, null));
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not written due to error.", t));
        }
    }

    /**
     * Encodes the given frame data into a ProtonBuffer but does not write the data until the next non-deferred
     * send if initiated.
     *
     * @param channel
     *      The channel to use when writing the frame
     * @param performative
     *      The SASL Performative to write
     */
    public void deferSaslFrame(int channel, DescribedType performative) {
        // When the outcome of SASL is written the decoder should revert to initial state
        // as the only valid next incoming value is an AMQP header.
        if (performative instanceof SaslOutcome) {
            frameParser.resetToExpectingHeader();
        }

        LOG.trace("{} Deferring SASL performative write: {}", driverName, performative);

        try {
            deferredWrites.add(frameEncoder.handleWrite(performative, channel));
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not written due to error.", t));
        }
    }

    /**
     * Encodes the given Header data into a ProtonBuffer but does not write the data until the next non-deferred
     * send if initiated.
     *
     * @param header
     *      The byte array to send as the AMQP Header.
     */
    public void deferHeader(AMQPHeader header) {
        LOG.trace("{} Deferring AMQP Header write: {}", driverName, header);
        try {
            deferredWrites.add(ByteBuffer.wrap(Arrays.copyOf(header.getBuffer(), AMQPHeader.HEADER_SIZE_BYTES)));
        } catch (Throwable t) {
            signalFailure(new AssertionError("Frame was not consumed due to error.", t));
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
     * @param splitWrite
     * 		Should the data be written in multiple chunks
     */
    public void sendAMQPFrame(int channel, DescribedType performative, ByteBuffer payload, boolean splitWrite) {
        LOG.trace("{} Sending performative: {}", driverName, performative);

        if (performative instanceof PerformativeDescribedType) {
            switch (((PerformativeDescribedType) performative).getPerformativeType()) {
                case OPEN:
                    localOpen = (Open) performative;
                default:
                    break;
            }
        }

        final ByteBuffer output;
        final ByteBuffer buffer = frameEncoder.handleWrite(performative, channel, payload, null);

        if (!deferredWrites.isEmpty()) {
            deferredWrites.add(buffer);
            try {
                output = composeDefferedWrites(deferredWrites).asReadOnlyBuffer();
            } finally {
                deferredWrites.clear();
            }
            LOG.trace("{} appending deferred buffer {} to next write.", driverName, output);
        } else {
            output = buffer;
        }

        try {
           LOG.trace("{} Writing out buffer {} to consumer: {}", driverName, output, frameConsumer);

            if (splitWrite) {
                final int bufferSplitPoint = output.remaining() / 2;

                final byte[] front = new byte[bufferSplitPoint - output.position()];
                final byte[] rear = new byte[output.remaining() - bufferSplitPoint];

                output.get(front);
                output.get(rear);

                frameConsumer.accept(ByteBuffer.wrap(front));
                frameConsumer.accept(ByteBuffer.wrap(rear));
            } else {
                frameConsumer.accept(output);
            }
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

        final ByteBuffer output;

        try {
            final ByteBuffer buffer = frameEncoder.handleWrite(performative, channel);

            if (!deferredWrites.isEmpty()) {
                deferredWrites.add(buffer);
                try {
                    output = composeDefferedWrites(deferredWrites).asReadOnlyBuffer();
                } finally {
                    deferredWrites.clear();
                }
                LOG.trace("{} appending deferred buffer {} to next write.", driverName, output);
            } else {
                output = buffer;
            }

            LOG.trace("{} Sending SASL performative: {}", driverName, performative);

            frameConsumer.accept(output);
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
        try {
            final ByteBuffer output;

            if (!deferredWrites.isEmpty()) {
                LOG.trace("{} appending deferred buffer {} to next write.", driverName, deferredWrites);
                deferredWrites.add(ByteBuffer.wrap(header.getBuffer()).asReadOnlyBuffer());

                try {
                    output = composeDefferedWrites(deferredWrites).asReadOnlyBuffer();
                } finally {
                    deferredWrites.clear();
                }
            } else {
                output = ByteBuffer.wrap(header.getBuffer()).asReadOnlyBuffer();
            }

            LOG.trace("{} Sending AMQP Header: {}", driverName, header);
            frameConsumer.accept(output);
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
        try {
            frameConsumer.accept(frameEncoder.handleWrite(null, channel, null, null));
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

    private static final ByteBuffer composeDefferedWrites(List<ByteBuffer> deferredWrites) {
        int totalSize = 0;
        for (ByteBuffer component : deferredWrites) {
            totalSize += component.remaining();
        }

        final ByteBuffer composite = ByteBuffer.allocate(totalSize);
        for (ByteBuffer component : deferredWrites) {
            composite.put(component);
        }

        deferredWrites.clear();

        return composite.flip().asReadOnlyBuffer();
    }

    private void searchForScriptioCompletionAndTrigger() {
        script.forEach(element -> {
            if (element instanceof ScriptCompleteAction) {
                ScriptCompleteAction completed = (ScriptCompleteAction) element;
                completed.perform(this);
            }
        });
    }

    private void processScript(ScriptedElement current) {
        if (current.performAfterwards() != null && failureCause == null) {
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
