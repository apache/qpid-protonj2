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
package org.apache.qpid.protonj2.engine.impl;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.engine.AMQPPerformativeEnvelopePool;
import org.apache.qpid.protonj2.engine.ConnectionState;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EnginePipeline;
import org.apache.qpid.protonj2.engine.EngineSaslDriver;
import org.apache.qpid.protonj2.engine.EngineState;
import org.apache.qpid.protonj2.engine.EventHandler;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.OutgoingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineNotStartedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineNotWritableException;
import org.apache.qpid.protonj2.engine.exceptions.EngineShutdownException;
import org.apache.qpid.protonj2.engine.exceptions.EngineStartedException;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.apache.qpid.protonj2.engine.exceptions.IdleTimeoutException;
import org.apache.qpid.protonj2.engine.exceptions.ProtonExceptionSupport;
import org.apache.qpid.protonj2.logging.ProtonLogger;
import org.apache.qpid.protonj2.logging.ProtonLoggerFactory;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Performative;

/**
 * The default proton Engine implementation.
 */
public class ProtonEngine implements Engine {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonEngine.class);

    private static final ProtonBuffer EMPTY_FRAME_BUFFER =
        ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00});

    private final ProtonEnginePipeline pipeline =  new ProtonEnginePipeline(this);
    private final ProtonEnginePipelineProxy pipelineProxy = new ProtonEnginePipelineProxy(pipeline);
    private final ProtonEngineConfiguration configuration = new ProtonEngineConfiguration(this);
    private final ProtonConnection connection = new ProtonConnection(this);
    private final AMQPPerformativeEnvelopePool<OutgoingAMQPEnvelope> framePool = AMQPPerformativeEnvelopePool.outgoingEnvelopePool();

    private EngineSaslDriver saslDriver = new ProtonEngineNoOpSaslDriver();

    private boolean writable;
    private EngineState state = EngineState.IDLE;
    private Throwable failureCause;
    private int inputSequence;
    private int outputSequence;

    // Idle Timeout Check data
    private ScheduledFuture<?> nextIdleTimeoutCheck;
    private ScheduledExecutorService idleTimeoutExecutor;
    private int lastInputSequence;
    private int lastOutputSequence;
    private long localIdleDeadline = 0;
    private long remoteIdleDeadline = 0;

    // Engine event points
    private BiConsumer<ProtonBuffer, Runnable> outputHandler;
    private EventHandler<Engine> engineShutdownHandler;
    private EventHandler<Engine> engineFailureHandler = (engine) -> {
        LOG.warn("Engine encountered error and will become inoperable: ", engine.failureCause());
    };

    @Override
    public ProtonConnection connection() {
        return connection;
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public boolean isRunning() {
        return state == EngineState.STARTED;
    }

    @Override
    public boolean isShutdown() {
        return state.ordinal() >= EngineState.SHUTDOWN.ordinal();
    }

    @Override
    public boolean isFailed() {
        return failureCause != null;
    }

    @Override
    public Throwable failureCause() {
        return failureCause;
    }

    @Override
    public EngineState state() {
        return state;
    }

    @Override
    public ProtonConnection start() throws EngineStateException {
        checkShutdownOrFailed("Cannot start an Engine that has already been shutdown or has failed.");

        if (state == EngineState.IDLE) {
            state = EngineState.STARTING;
            try {
                pipeline.fireEngineStarting();
                state = EngineState.STARTED;
                writable = true;
                connection.handleEngineStarted(this);
            } catch (Throwable error) {
                throw engineFailed(error);
            }
        }

        return connection;
    }

    @Override
    public ProtonEngine shutdown() {
        if (state.ordinal() < EngineState.SHUTTING_DOWN.ordinal()) {
            state = EngineState.SHUTDOWN;
            writable = false;

            if (nextIdleTimeoutCheck != null) {
                LOG.trace("Canceling scheduled Idle Timeout Check");
                nextIdleTimeoutCheck.cancel(false);
                nextIdleTimeoutCheck = null;
            }

            try {
                pipeline.fireEngineStateChanged();
            } catch (Exception ignored) {}

            try {
                connection.handleEngineShutdown(this);
            } catch (Exception ignored) {
            } finally {
                if (engineShutdownHandler != null) {
                    engineShutdownHandler.handle(this);
                }
            }
        }

        return this;
    }

    @Override
    public long tick(long currentTime) throws IllegalStateException, EngineStateException {
        checkShutdownOrFailed("Cannot tick an Engine that has been shutdown or failed.");

        if (connection.getState() != ConnectionState.ACTIVE) {
            throw new IllegalStateException("Cannot tick on a Connection that is not opened or an engine that has been shut down.");
        }

        if (idleTimeoutExecutor != null) {
            throw new IllegalStateException("Automatic ticking previously initiated.");
        }

        performReadCheck(currentTime);
        performWriteCheck(currentTime);

        return nextTickDeadline(localIdleDeadline, remoteIdleDeadline);
    }

    @Override
    public ProtonEngine tickAuto(ScheduledExecutorService executor) throws IllegalStateException, EngineStateException {
        checkShutdownOrFailed("Cannot start auto tick on an Engine that has been shutdown or failed");

        Objects.requireNonNull(executor);

        if (connection.getState() != ConnectionState.ACTIVE) {
            throw new IllegalStateException("Cannot tick on a Connection that is not opened.");
        }

        if (idleTimeoutExecutor != null) {
            throw new IllegalStateException("Automatic ticking previously initiated.");
        }

        // TODO - As an additional feature of this method we could allow for calling before connection is
        //        opened such that it starts ticking either on open local and also checks as a response to
        //        remote open which seems might be needed anyway, see notes in IdleTimeoutCheck class.

        // Immediate run of the idle timeout check logic will decide afterwards when / if we should
        // reschedule the idle timeout processing.
        LOG.trace("Auto Idle Timeout Check being initiated");
        idleTimeoutExecutor = executor;
        idleTimeoutExecutor.execute(new IdleTimeoutCheck());

        return this;
    }

    @Override
    public ProtonEngine ingest(ProtonBuffer input) throws EngineStateException {
        checkShutdownOrFailed("Cannot ingest data into an Engine that has been shutdown or failed");

        if (!isWritable()) {
            throw new EngineNotWritableException("Engine is currently not accepting new input");
        }

        try {
            int startIndex = input.getReadIndex();
            pipeline.fireRead(input);
            if (input.getReadIndex() != startIndex) {
                inputSequence++;
            }
        } catch (Exception error) {
            throw engineFailed(error);
        }

        return this;
    }

    @Override
    public EngineStateException engineFailed(Throwable cause) {
        final EngineStateException failure;

        if (state.ordinal() < EngineState.SHUTTING_DOWN.ordinal() && state != EngineState.FAILED) {
            state = EngineState.FAILED;
            failureCause = cause;
            writable = false;

            if (nextIdleTimeoutCheck != null) {
                LOG.trace("Canceling scheduled Idle Timeout Check");
                nextIdleTimeoutCheck.cancel(false);
                nextIdleTimeoutCheck = null;
            }

            failure = ProtonExceptionSupport.createFailedException(cause);

            try {
                pipeline.fireFailed((EngineFailedException) failure);
            } catch (Exception ignored) {}

            try {
                connection.handleEngineFailed(this, cause);
            } catch (Exception ignored) {
            }

            engineFailureHandler.handle(this);
        } else {
            if (isFailed()) {
                failure = ProtonExceptionSupport.createFailedException(cause);
            } else {
                failure = new EngineShutdownException("Engine has transitioned to shutdown state");
            }
        }

        return failure;
    }

    //----- Engine configuration

    @Override
    public ProtonEngine outputHandler(BiConsumer<ProtonBuffer, Runnable> handler) {
        this.outputHandler = handler;
        return this;
    }

    BiConsumer<ProtonBuffer, Runnable> outputHandler() {
        return outputHandler;
    }

    @Override
    public ProtonEngine errorHandler(EventHandler<Engine> handler) {
        this.engineFailureHandler = handler;
        return this;
    }

    EventHandler<Engine> errorHandler() {
        return engineFailureHandler;
    }

    @Override
    public ProtonEngine shutdownHandler(EventHandler<Engine> handler) {
        this.engineShutdownHandler = handler;
        return this;
    }

    EventHandler<Engine> engineShutdownHandler() {
        return engineShutdownHandler;
    }

    @Override
    public EnginePipeline pipeline() {
        return pipelineProxy;
    }

    @Override
    public ProtonEngineConfiguration configuration() {
        return configuration;
    }

    @Override
    public EngineSaslDriver saslDriver() {
        return saslDriver;
    }

    /**
     * Allows for registration of a custom {@link EngineSaslDriver} that will convey
     * SASL state and configuration for this engine.
     *
     * @param saslDriver
     *      The {@link EngineSaslDriver} that this engine will use.
     *
     * @throws EngineStateException if the engine state doesn't allow for changes
     */
    public void registerSaslDriver(EngineSaslDriver saslDriver) throws EngineStateException {
        checkShutdownOrFailed("Cannot register a SASL driver on an Engine that is shutdown or failed.");

        if (state.ordinal() > EngineState.STARTING.ordinal()) {
            throw new EngineStartedException("Cannot alter SASL driver after Engine has been started.");
        }

        this.saslDriver = saslDriver;
    }

    //----- Internal proton engine implementation

    ProtonEngine fireWrite(HeaderEnvelope frame) {
        pipeline.fireWrite(frame);
        return this;
    }

    ProtonEngine fireWrite(OutgoingAMQPEnvelope frame) {
        pipeline.fireWrite(frame);
        return this;
    }

    ProtonEngine fireWrite(Performative performative, int channel) {
        pipeline.fireWrite(framePool.take(performative, channel, null));
        return this;
    }

    ProtonEngine fireWrite(Performative performative, int channel, ProtonBuffer payload) {
        pipeline.fireWrite(framePool.take(performative, channel, payload));
        return this;
    }

    OutgoingAMQPEnvelope wrap(Performative performative, int channel, ProtonBuffer payload) {
        return framePool.take(performative, channel, payload);
    }

    void checkEngineNotStarted(String message) {
        if (state == EngineState.IDLE) {
            throw new EngineNotStartedException(message);
        }
    }

    void checkFailed(String message) {
        if (state == EngineState.FAILED) {
            throw ProtonExceptionSupport.createFailedException(message, failureCause);
        }
    }

    void checkShutdownOrFailed(String message) {
        if (state.ordinal() > EngineState.STARTED.ordinal()) {
            if (isFailed()) {
                throw ProtonExceptionSupport.createFailedException(message, failureCause);
            } else {
                throw new EngineShutdownException(message);
            }
        }
    }

    void dispatchWriteToEventHandler(ProtonBuffer buffer, Runnable ioComplete) {
        if (outputHandler != null) {
            outputSequence++;
            try {
                outputHandler.accept(buffer, ioComplete);
            } catch (Throwable error) {
                throw engineFailed(error);
            }
        } else {
            throw engineFailed(new IllegalStateException("No output handler configured"));
        }
    }

    //----- Idle Timeout processing methods and inner classes

    private void performReadCheck(long currentTime) {
        long localIdleTimeout = connection.getIdleTimeout();

        if (localIdleTimeout > 0) {
            if (localIdleDeadline == 0 || lastInputSequence != inputSequence) {
                localIdleDeadline = computeDeadline(currentTime, localIdleTimeout);
                lastInputSequence = inputSequence;
            } else if (localIdleDeadline - currentTime <= 0) {
                if (connection.getState() != ConnectionState.CLOSED) {
                    ErrorCondition condition = new ErrorCondition(
                        Symbol.getSymbol("amqp:resource-limit-exceeded"), "local-idle-timeout expired");
                    connection.setCondition(condition);
                    connection.close();
                    engineFailed(new IdleTimeoutException("Remote idle timeout detected"));
                } else {
                    localIdleDeadline = computeDeadline(currentTime, localIdleTimeout);
                }
            }
        }
    }

    private void performWriteCheck(long currentTime) {
        long remoteIdleTimeout = connection.getRemoteIdleTimeout();

        if (remoteIdleTimeout > 0 && !connection.isLocallyClosed()) {
            if (remoteIdleDeadline == 0 || lastOutputSequence != outputSequence) {
                remoteIdleDeadline = computeDeadline(currentTime, remoteIdleTimeout / 2);
                lastOutputSequence = outputSequence;
            } else if (remoteIdleDeadline - currentTime <= 0) {
                remoteIdleDeadline = computeDeadline(currentTime, remoteIdleTimeout / 2);
                pipeline.fireWrite(EMPTY_FRAME_BUFFER.duplicate(), null);
                lastOutputSequence++;
            }
        }
    }

    private long computeDeadline(long now, long timeout) {
        long deadline = now + timeout;
        // We use 0 to signal not-initialized and/or no-timeout, so in the
        // unlikely event that's to be the actual deadline, return 1 instead
        return deadline != 0 ? deadline : 1;
    }

    private static long nextTickDeadline(long localIdleDeadline, long remoteIdleDeadline) {
        final long deadline;

        // If there is no locally set idle timeout then we just honor the remote idle timeout
        // value otherwise we need to use the lesser of the next local or remote idle timeout
        // deadline values to compute the next time a check is needed.
        if (localIdleDeadline == 0) {
             deadline = remoteIdleDeadline;
        } else if (remoteIdleDeadline == 0) {
            deadline = localIdleDeadline;
        } else {
            if (remoteIdleDeadline - localIdleDeadline <= 0) {
                deadline = remoteIdleDeadline;
            } else {
                deadline = localIdleDeadline;
            }
        }

        return deadline;
    }

    private final class IdleTimeoutCheck implements Runnable {

        // TODO - Pick reasonable values
        private final long MIN_IDLE_CHECK_INTERVAL = 1000;
        private final long MAX_IDLE_CHECK_INTERVAL = 10000;

        @Override
        public void run() {
            boolean checkScheduled = false;

            if (connection.getState() == ConnectionState.ACTIVE && !isShutdown()) {
                // Using nano time since it is not related to the wall clock, which may change
                long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

                try {
                    performReadCheck(now);
                    performWriteCheck(now);

                    final long deadline = nextTickDeadline(localIdleDeadline, remoteIdleDeadline);

                    // Check methods will close down the engine and fire error so we need to check that engine
                    // state is active and engine is not shutdown before scheduling again.
                    if (deadline != 0 && connection.getState() == ConnectionState.ACTIVE && state() == EngineState.STARTED) {
                        // Run the next idle check at half the deadline to try and ensure we meet our
                        // obligation of sending our heart beat on time.
                        long delay = (deadline - now) / 2;

                        // TODO - Some computation to work out a reasonable delay that still compensates for
                        //        errors in scheduling while preventing over eagerness.
                        delay = Math.max(MIN_IDLE_CHECK_INTERVAL, delay);
                        delay = Math.min(MAX_IDLE_CHECK_INTERVAL, delay);

                        checkScheduled = true;
                        LOG.trace("IdleTimeoutCheck rescheduling with delay: {}", delay);
                        nextIdleTimeoutCheck = idleTimeoutExecutor.schedule(this, delay, TimeUnit.MILLISECONDS);
                    }

                    // TODO - If no local timeout but remote hasn't opened we might return zero and not
                    //        schedule any ticking ?  Possible solution is to schedule after remote open
                    //        arrives if nothing set to run and remote indicates it has an idle timeout.

                } catch (Throwable t) {
                    LOG.trace("Auto Idle Timeout Check encountered error during check: ", t);
                }
            }

            if (!checkScheduled) {
                nextIdleTimeoutCheck = null;
                LOG.trace("Auto Idle Timeout Check task exiting and will not be rescheduled");
            }
        }
    }
}
