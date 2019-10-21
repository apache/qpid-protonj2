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
package org.apache.qpid.proton4j.engine;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineShutdownException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;

/**
 * AMQP Engine interface.
 */
public interface Engine extends Consumer<ProtonBuffer> {

    /**
     * Returns true if the engine is accepting more input.
     * <p>
     * When false any attempts to write more data into the engine will result in an
     * error being returned from the write operation.
     *
     * TODO What is the best why to reflect the state, how do we trigger changes
     *
     * @return true if the engine is current accepting more input.
     */
    boolean isWritable();

    /**
     * @return true if the Engine has been shutdown and is no longer usable.
     */
    boolean isShutdown();

    /**
     * @return true if the Engine has encountered a critical error and has shutdown.
     */
    boolean isFailed();

    /**
     * @return the error that caused the {@link Engine} fail and shutdown (or null if not failed).
     */
    Throwable failureCause();

    /**
     * @return the current state of the engine.
     */
    EngineState state();

    /**
     * Gets the {@link Connection} instance that is associated with this {@link Engine} instance.
     * It is valid for an engine implementation to not return a {@link Connection} instance prior
     * to the engine having been started.
     *
     * @return the {@link Connection} that is linked to this engine instance.
     */
    Connection connection();

    /**
     * Starts the engine and returns the {@link Connection} instance that is bound to this Engine.
     *
     * @return the Connection instance that is linked to this {@link Engine}
     *
     * @throws EngineStateException if the Engine state has already transition to shutdown or failed.
     */
    Connection start() throws EngineStateException;

    /**
     * Orderly shutdown of the engine, any open connection and associated sessions and
     * session linked end points will be closed.
     *
     * @return this Engine
     */
    Engine shutdown();

    /**
     * Transition the {@link Engine} to a failed state if not already closed or closing.
     *
     * If called when the engine has not failed the engine will be shutdown and transition to
     * a failed state and the method will return an appropriate {@link EngineFailedException}
     * that wraps the given cause.  If called after the engine was shutdown the method returns
     * an {@link EngineShutdownException} indicating that the engine was already shutdown.
     *
     * @param cause
     *      The exception that caused the engine to be forcibly transitioned to the failed state.
     *
     * @return an {@link EngineStateException} that can be thrown indicating the failure and engine state.
     */
    EngineStateException engineFailed(Throwable cause);

    /**
     * Provide data input for this Engine from some external source.
     *
     * @param input
     *      The data to feed into to Engine.
     *
     * @return this Engine
     *
     * @throws EngineStateException if the Engine state precludes accepting new input.
     */
    Engine ingest(ProtonBuffer input) throws EngineStateException;

    /**
     * Provide data input for this Engine from some external source.
     *
     * @param input
     *      The data to feed into to Engine.
     *
     * @throws EngineStateException if the Engine state precludes accepting new input.
     */
    @Override
    default void accept(ProtonBuffer input) throws EngineStateException {
        ingest(input);
    }

    /**
     * Prompt the engine to perform idle-timeout/heartbeat handling, and return an absolute
     * deadline in milliseconds that tick must again be called by/at, based on the provided
     * current time in milliseconds, to ensure the periodic work is carried out as necessary.
     * It is an error to call this method if the connection has not been opened.
     *
     * A returned deadline of 0 indicates there is no periodic work necessitating tick be called, e.g.
     * because neither peer has defined an idle-timeout value.
     *
     * The provided milliseconds time values should be derived from a monotonic source such as
     * {@link System#nanoTime()} to prevent wall clock changes leading to erroneous behaviour. Note
     * that for {@link System#nanoTime()} derived values in particular that the returned deadline
     * could be a different sign than the originally given value, and so (if non-zero) the returned
     * deadline should have the current time originally provided subtracted from it in order to
     * establish a relative time delay to the next deadline.
     *
     * Supplying {@link System#currentTimeMillis()} derived values can lead to erroneous behaviour
     * during wall clock changes and so is not recommended.
     *
     * It is an error to call this method if {@link Engine#tickAuto(ScheduledExecutorService)} was called.
     *
     * @param currentTime
     *      the current time of this tick call.
     *
     * @return the absolute deadline in milliseconds to next call tick by/at, or 0 if there is none.
     *
     * @throws IllegalStateException if the {@link Engine} is already performing auto tick handling.
     * @throws EngineStateException if the Engine state precludes accepting new input.
     */
    long tick(long currentTime) throws IllegalStateException, EngineStateException;

    /**
     * Allows the engine to manage idle timeout processing by providing it the single threaded executor
     * context where all transport work is done which ensures singled threaded access while removing the
     * need for the client library or server application to manage calls to the {@link Engine#tick} methods.
     *
     * @param executor
     *      The single threaded execution context where all engine work takes place.
     *
     * @throws IllegalStateException if the {@link Engine} is already performing auto tick handling.
     * @throws EngineStateException if the Engine state precludes accepting new input.
     */
    void tickAuto(ScheduledExecutorService executor) throws IllegalStateException, EngineStateException;

    /**
     * Gets the EnginePipeline for this Engine.
     *
     * @return the {@link EnginePipeline} for this {@link Engine}.
     */
    EnginePipeline pipeline();

    /**
     * Gets the Configuration for this engine.
     *
     * @return the configuration object for this engine.
     */
    EngineConfiguration configuration();

    /**
     * Gets the SASL context for this engine, if no SASL layer is configured then a
     * default no-op context must be returned that indicates this.
     *
     * @return the SASL context for the engine.
     */
    EngineSaslDriver saslContext();

    //----- Engine event points

    /**
     * Sets a handler instance that will be notified when data from the engine is ready to
     * be written to some output sink (socket etc).
     *
     * @param output
     *      The {@link ProtonBuffer} handler instance that performs IO for the engine output.
     *
     * @return this Engine
     */
    Engine outputHandler(EventHandler<ProtonBuffer> output);

    /**
     * Sets a {@link Consumer} instance that will be notified when data from the engine is ready to
     * be written to some output sink (socket etc).
     *
     * @param consumer
     *      The {@link ProtonBuffer} consumer instance that performs IO for the engine output.
     *
     * @return this Engine
     */
    default Engine outputConsumer(Consumer<ProtonBuffer> consumer) {
        outputHandler((output) -> {
            consumer.accept(output);
        });

        return this;
    }

    /**
     * Sets a handler instance that will be notified when the engine encounters a fatal error.
     *
     * @param engineFailure
     *      The {@link ProtonException} handler instance that will be notified if the engine fails.
     *
     * @return this Engine
     */
    Engine errorHandler(EventHandler<Throwable> engineFailure);

}
