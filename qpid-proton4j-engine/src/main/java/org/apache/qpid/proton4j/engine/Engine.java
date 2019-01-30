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

/**
 * AMQP Engine interface.
 */
public interface Engine {

    // TODO

    Connection start();

    void shutdown();

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
