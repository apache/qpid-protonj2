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

    /**
     * @return the current state of the engine.
     */
    EngineState getState();

    //----- Engine control APIs

    /**
     * Starts the engine and returns a Connection that is bound to this Engine.
     *
     * @return the Connection bound to this engine.
     *
     * TODO - How do we want to surface the Connection, here or via some factory that
     *        create an engine and binds it to a Connection etc ?
     */
    Connection start();

    /**
     * Orderly shutdown of the engine, any open connection and associated sessions and
     * session linked enpoints will be closed.
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
     * TODO - Exception thrown here.
     * TODO - Do we want a simple high level input or leave it to some handler etc ?
     */
    void ingest(ProtonBuffer input);

    //----- Engine configuration and state

    /**
     * Sets a EngineListener to be notified of events on this Engine
     *
     * @param listener
     *      The EngineListener to notify
     */
    void setEngineListener(EngineListener listener);

    /**
     * Gets the currently configured EngineListener instance.
     *
     * @return the currently configured EngineListener.
     */
    EngineListener getEngineListener();

    /**
     * Gets the EnginePipeline for this Engine.
     *
     * @return the {@link EnginePipeline} for this {@link Engine}.
     */
    EnginePipeline getPipeline();

    /**
     * Gets the Configuration for this engine.
     *
     * @return the configuration object for this engine.
     */
    EngineConfiguration getConfiguration();

    /**
     * Gets the SASL context for this engine, if not SASL layer is configured then a
     * defacult no-op context should be returned that indicates this.
     *
     * @return the SASL context for the engine.
     */
    EngineSaslContext getSaslContext();

}
