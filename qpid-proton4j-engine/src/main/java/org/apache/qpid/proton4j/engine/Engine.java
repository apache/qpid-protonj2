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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.exceptions.EngineClosedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineNotWritableException;
import org.apache.qpid.proton4j.engine.exceptions.EngineStateException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;

/**
 * AMQP Engine interface.
 */
public interface Engine {

    //----- Engine state

    /**
     * Returns true if the engine is accepting more input.
     * <p>
     * When false any attempts to write more data into the engine will result in an
     * error being returned from the write operation.
     *
     * TODO What is the best why to reflect the error, how do we trigger changes
     *
     * @return true if the engine is current accepting more input.
     */
    boolean isWritable();

    // TODO decide on terminology for state, shutdown, closed, etc

    /**
     * @return true if the Engine has been shutdown and is no longer usable.
     */
    boolean isShutdown();

    /**
     * @return the current state of the engine.
     */
    EngineState state();

    //----- Engine control APIs

    /**
     * Starts the engine
     * <p>
     * Once started the engine will signal the provided handler that the engine is started and
     * provide a new Connection instance.
     *
     * @param handler
     *      A handler that will be notified when the engine has started and has a valid Connection.
     */
    void start(EventHandler<AsyncEvent<Connection>> handler);

    /**
     * Orderly shutdown of the engine, any open connection and associated sessions and
     * session linked enpoints will be closed.
     *
     * TODO - Errors
     */
    void shutdown();

    /**
     * Provide data input for this Engine from some external source.
     * <p>
     * The provided {@link ProtonBuffer} should be a non-pooled buffer which the Engine
     * will take ownership of and should not be modified or reused by the caller.
     *
     * @param input
     *      The data to feed into to Engine.
     *
     * @throws EngineNotWritableException if the Engine is not accepting new input.
     * @throws EngineClosedException if the Engine has been shutdown or has failed.
     */
    void ingest(ProtonBuffer input) throws EngineStateException;

    // TODO - Do we need to accept time here or can we just use a time value of our own choosing
    //        as this isn't C so we have the same clock values available as a caller would.
    long tick(long currentTime);

    //----- Engine configuration and state

    /**
     * Sets a handler instance that will be notified when data from the engine is ready to
     * be written to some output sink (socket etc).
     *
     * @param output
     *      The {@link ProtonBuffer} handler instance that perform IO for the engine output.
     */
    void outputHandler(EventHandler<ProtonBuffer> output);

    /**
     * Sets a handler instance that will be notified when the engine encounters a fatal error.
     *
     * @param engineFailure
     *      The {@link ProtonException} handler instance that will be notified if the engine fails.
     */
    void errorHandler(EventHandler<ProtonException> engineFailure);

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
     * Gets the SASL context for this engine, if not SASL layer is configured then a
     * default no-op context should be returned that indicates this.
     *
     * @return the SASL context for the engine.
     */
    EngineSaslContext saslContext();

}
