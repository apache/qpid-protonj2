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
import org.apache.qpid.proton4j.engine.EngineSaslContext;
import org.apache.qpid.proton4j.engine.EngineState;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.exceptions.EngineClosedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineIdleTimeoutException;
import org.apache.qpid.proton4j.engine.exceptions.EngineNotWritableException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;
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

    private EngineSaslContext saslContext = new ProtonEngineNoOpSaslContext();

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
    private EventHandler<ProtonException> engineErrorHandler = (error) -> {
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
    public long tick() throws EngineStateException {
        return tick(System.nanoTime());
    }

    @Override
    public long tick(long currentTime) throws EngineStateException {
        long deadline = 0;
        long localIdleTimeout = connection.getIdleTimeout();
        long remoteIdleTimeout = connection.getRemoteIdleTimeout();

        if (localIdleTimeout > 0) {
            if (localIdleDeadline == 0 || lastInputSequence != inputSequence) {
                localIdleDeadline = computeDeadline(currentTime, localIdleTimeout);
                lastInputSequence = inputSequence;
            } else if (localIdleDeadline - currentTime <= 0) {
                localIdleDeadline = computeDeadline(currentTime, localIdleTimeout);
                if (connection.getLocalState() != ConnectionState.CLOSED) {
                    ErrorCondition condition = new ErrorCondition(
                        Symbol.getSymbol("amqp:resource-limit-exceeded"), "local-idle-timeout expired");
                    connection.setLocalCondition(condition);
                    connection.open();  // Ensure open sent and state ready for close call.
                    connection.close();
                    // TODO - What about SASL layer ?
                    // saslContext().fail(); ??
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
                deadline = remoteIdleDeadline;
            } else {
                if (remoteIdleDeadline - localIdleDeadline <= 0) {
                    deadline = remoteIdleDeadline;
                } else {
                    deadline = localIdleDeadline;
                }
            }
        }

        return deadline;
    }

    @Override
    public void autoTick(ScheduledExecutorService executor) throws EngineStateException {
        if (isShutdown()) {
            throw new EngineClosedException("The engine has already shut down.");
        }

        this.idleTimeoutExecutor = executor;

        // Using nano time since it is not related to the wall clock, which may change
        long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long deadline = tick(now);
        if (deadline != 0) {
            long delay = deadline - now;
            LOG.trace("Idle Timeout Check being initiated, initial delay: {}", delay);
            nextIdleTimeoutCheck = idleTimeoutExecutor.schedule(new IdleTimeoutCheck(), delay, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public ProtonEngine ingest(ProtonBuffer input) throws EngineStateException {
        if (isShutdown()) {
            throw new EngineClosedException("The engine has already shut down.");
        }

        inputSequence++;

        if (!isWritable()) {
            throw new EngineNotWritableException("Engine is currently not accepting new input");
        }

        try {
            pipeline.fireRead(input);
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
    public ProtonEngine errorHandler(EventHandler<ProtonException> handler) {
        this.engineErrorHandler = handler;
        return this;
    }

    EventHandler<ProtonException> errorHandler() {
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
    public EngineSaslContext saslContext() {
        return saslContext;
    }

    /**
     * Allows for registration of a custom {@link EngineSaslContext} that will convey
     * SASL state and configuration for this engine.
     *
     * @param saslContext
     *      The {@link EngineSaslContext} that this engine will use.
     *
     * @throws EngineStateException if the engine state doesn't allow for changes
     */
    public void registerSaslContext(EngineSaslContext saslContext) throws EngineStateException {
        if (state.ordinal() > EngineState.STARTING.ordinal()) {
            throw new EngineStateException("Cannot alter SASL context after engine has been started.");
        }

        this.saslContext = saslContext;
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

    void engineFailed(ProtonException cause) {
        state = EngineState.FAILED;
        writable = false;

        if (nextIdleTimeoutCheck != null) {
            LOG.trace("Cancelling scheduled Idle Timeout Check");
            nextIdleTimeoutCheck.cancel(false);
            nextIdleTimeoutCheck = null;
        }

        engineErrorHandler.handle(cause);
    }

    private long computeDeadline(long now, long timeout) {
        long deadline = now + timeout;
        // We use 0 to signal not-initialised and/or no-timeout, so in the
        // unlikely event thats to be the actual deadline, return 1 instead
        return deadline != 0 ? deadline : 1;
    }

    //----- Utility classes for internal use only.

    private final class IdleTimeoutCheck implements Runnable {

        @Override
        public void run() {
            boolean checkScheduled = false;

            if (connection.getLocalState() == ConnectionState.ACTIVE) {
                // Using nano time since it is not related to the wall clock, which may change
                long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

                try {
                    long deadline = tick(now);

                    if (connection.getLocalState() == ConnectionState.CLOSED) {
                        LOG.info("Idle Timeout Check closed the Engine due to the peer exceeding our requested idle-timeout.");
                        engineFailed(new EngineIdleTimeoutException(
                            "Engine shutdown due to the peer exceeding our requested idle-timeout"));
                    } else {
                        if (deadline != 0) {
                            long delay = deadline - now;
                            checkScheduled = true;
                            LOG.trace("IdleTimeoutCheck rescheduling with delay: {}", delay);
                            nextIdleTimeoutCheck = idleTimeoutExecutor.schedule(this, delay, TimeUnit.MILLISECONDS);
                        }
                    }
                } catch (Throwable t) {
                    LOG.trace("Engine Idle Check encountered error during check: ", t);
                }
            } else {
                LOG.trace("IdleTimeoutCheck skipping check, connection is not active.");
            }

            if (!checkScheduled) {
                nextIdleTimeoutCheck = null;
                LOG.trace("Idle Timeout Check task exiting and will not be rescheduled");
            }
        }
    }
}
