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
package org.apache.qpid.proton4j.engine.impl;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.engine.ConnectionState;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineSaslDriver;
import org.apache.qpid.proton4j.engine.EngineState;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.exceptions.EngineClosedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineIdleTimeoutException;
import org.apache.qpid.proton4j.engine.exceptions.EngineNotWritableException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonExceptionSupport;

/**
 * The default proton4j Engine implementation.
 */
public class ProtonEngine implements Engine {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(ProtonEngine.class);

    private static final ProtonBuffer EMPTY_FRAME_BUFFER =
        ProtonByteBufferAllocator.DEFAULT.wrap(new byte[] {0x00, 0x00, 0x00, 0x08, 0x02, 0x00, 0x00, 0x00});

    private final ProtonEnginePipeline pipeline =  new ProtonEnginePipeline(this);
    private final ProtonEngineConfiguration configuration = new ProtonEngineConfiguration(this);
    private final ProtonConnection connection = new ProtonConnection(this);

    private EngineSaslDriver saslDriver = new ProtonEngineNoOpSaslDriver();

    private boolean writable;
    private EngineState state = EngineState.IDLE;
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
    private EventHandler<ProtonBuffer> outputHandler;
    private EventHandler<Throwable> engineErrorHandler = (error) -> {
        LOG.warn("Engine encounted error and will shutdown: ", error);
    };

    @Override
    public ProtonConnection getConnection() {
        return connection;
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public boolean isShutdown() {
        return state == EngineState.SHUTDOWN;
    }

    @Override
    public EngineState state() {
        return state;
    }

    @Override
    public ProtonConnection start() {
        state = EngineState.STARTING;
        try {
            pipeline().fireEngineStarting();
            state = EngineState.STARTED;
            writable = true;
        } catch (Throwable error) {
            // TODO - Error types ?
            state = EngineState.FAILED;
            writable = false;

            throw error;
        }

        return connection;
    }

    @Override
    public ProtonEngine shutdown() {
        state = EngineState.SHUTDOWN;
        writable = false;

        // TODO - We aren't currently checking connection state, do we want to close if open ?

        // TODO - Once shutdown future calls that trigger output should not write anything
        //        or if they do the write should probably just no-op as we know we are already
        //        shut down and can't emit any frames.  Does this entail closing connection if
        //        still open ?

        if (nextIdleTimeoutCheck != null) {
            LOG.trace("Cancelling scheduled Idle Timeout Check");
            nextIdleTimeoutCheck.cancel(false);
            nextIdleTimeoutCheck = null;
        }

        return this;
    }

    @Override
    public long tick(long currentTime) {
        if (isShutdown() || connection.getState() != ConnectionState.ACTIVE) {
            throw new IllegalStateException("Cannot tick on a Connection that is not opened or an engine that has been shut down.");
        }

        if (idleTimeoutExecutor != null) {
            throw new IllegalStateException("Automatic ticking previously initiated.");
        }

        return performTick(currentTime);
    }

    @Override
    public void tickAuto(ScheduledExecutorService executor) {
        Objects.requireNonNull(executor);

        if (isShutdown() || connection.getState() != ConnectionState.ACTIVE) {
            throw new IllegalStateException("Cannot tick on a Connection that is not opened or an engine that has been shut down.");
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
    }

    @Override
    public ProtonEngine ingest(ProtonBuffer input) {
        if (isShutdown()) {
            throw new EngineClosedException("The engine has already shut down.");
        }

        if (!isWritable()) {
            throw new EngineNotWritableException("Engine is currently not accepting new input");
        }

        try {
            int startIndex = input.getReadIndex();
            pipeline.fireRead(input);
            if (input.getReadIndex() != startIndex) {
                inputSequence++;
            }
        } catch (Throwable t) {
            // TODO define what the pipeline does here as far as throwing vs signaling etc.
            engineFailed(ProtonExceptionSupport.create(t));
            throw t;
        }

        return this;
    }

    //----- Engine configuration

    @Override
    public ProtonEngine outputHandler(EventHandler<ProtonBuffer> handler) {
        this.outputHandler = handler;
        return this;
    }

    EventHandler<ProtonBuffer> outputHandler() {
        return outputHandler;
    }

    @Override
    public ProtonEngine errorHandler(EventHandler<Throwable> handler) {
        this.engineErrorHandler = handler;
        return this;
    }

    EventHandler<Throwable> errorHandler() {
        return engineErrorHandler;
    }

    @Override
    public ProtonEnginePipeline pipeline() {
        return pipeline;
    }

    @Override
    public ProtonEngineConfiguration configuration() {
        return configuration;
    }

    @Override
    public EngineSaslDriver saslContext() {
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
    public void registerSaslDriver(EngineSaslDriver saslDriver) {
        if (state.ordinal() > EngineState.STARTING.ordinal()) {
            throw new EngineStateException("Cannot alter SASL driver after engine has been started.");
        }

        this.saslDriver = saslDriver;
    }

    //----- Internal proton engine implementation

    void dispatchWriteToEventHandler(ProtonBuffer buffer) {
        if (outputHandler != null) {
            outputSequence++;
            outputHandler.handle(buffer);
        } else {
            engineFailed(new EngineStateException("No output handler configured"));
        }
    }

    void engineFailed(Throwable cause) {
        state = EngineState.FAILED;
        writable = false;

        if (nextIdleTimeoutCheck != null) {
            LOG.trace("Cancelling scheduled Idle Timeout Check");
            nextIdleTimeoutCheck.cancel(false);
            nextIdleTimeoutCheck = null;
        }

        engineErrorHandler.handle(cause);
    }

    private long performTick(long currentTime) {

        long deadline = 0;
        long localIdleTimeout = connection.getIdleTimeout();
        long remoteIdleTimeout = connection.getRemoteIdleTimeout();

        if (localIdleTimeout > 0) {
            if (localIdleDeadline == 0 || lastInputSequence != inputSequence) {
                localIdleDeadline = computeDeadline(currentTime, localIdleTimeout);
                lastInputSequence = inputSequence;
            } else if (localIdleDeadline - currentTime <= 0) {
                localIdleDeadline = computeDeadline(currentTime, localIdleTimeout);
                if (connection.getState() != ConnectionState.CLOSED) {
                    ErrorCondition condition = new ErrorCondition(
                        Symbol.getSymbol("amqp:resource-limit-exceeded"), "local-idle-timeout expired");
                    connection.setCondition(condition);
                    connection.close();
                    engineFailed(new EngineIdleTimeoutException("Remote idle timeout detected"));
                    return 0;
                }
            }
            deadline = localIdleDeadline;
        }

        if (remoteIdleTimeout != 0 && !connection.isLocallyClosed()) {
            if (remoteIdleDeadline == 0 || lastOutputSequence != outputSequence) {
                remoteIdleDeadline = computeDeadline(currentTime, remoteIdleTimeout / 2);
                lastOutputSequence = outputSequence;
            } else if (remoteIdleDeadline - currentTime <= 0) {
                remoteIdleDeadline = computeDeadline(currentTime, remoteIdleTimeout / 2);
                pipeline().fireWrite(EMPTY_FRAME_BUFFER.duplicate());
                lastOutputSequence++;
            }

            if (deadline == 0) {
                // There was no local deadline, so use whatever the remote is.
                deadline = remoteIdleDeadline;
            } else {
                // Use the 'earlier' of the remote and local deadline values
                if (remoteIdleDeadline - localIdleDeadline <= 0) {
                    deadline = remoteIdleDeadline;
                } else {
                    deadline = localIdleDeadline;
                }
            }
        }

        return deadline;
    }

    private long computeDeadline(long now, long timeout) {
        long deadline = now + timeout;
        // We use 0 to signal not-initialised and/or no-timeout, so in the
        // unlikely event thats to be the actual deadline, return 1 instead
        return deadline != 0 ? deadline : 1;
    }

    //----- Utility classes for internal use only.

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
                    long deadline = tick(now);

                    // Tick will close down the engine and fire error so we need to check that engine state is
                    // active and engine is not shutdown before scheduling again.
                    if (deadline != 0 && connection.getState() == ConnectionState.ACTIVE && !isShutdown()) {
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
