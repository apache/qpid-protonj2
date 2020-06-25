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
package org.apache.qpid.protonj2.engine;

import org.apache.qpid.protonj2.engine.impl.ProtonEngineFactory;

/**
 * Interface used to define the basic mechanisms for creating Engine instances.
 */
public interface EngineFactory {

    public static final EngineFactory PROTON = new ProtonEngineFactory();

    /**
     * Create a new Engine instance with a SASL authentication layer added.  The returned
     * Engine can either be fully pre-configured for SASL or can require additional user
     * configuration.
     *
     * @return a new Engine instance that can handle SASL authentication.
     */
    Engine createEngine();

    /**
     * Create a new Engine instance that handles only raw AMQP with no SASL layer enabled.
     *
     * @return a new raw AMQP aware Engine implementation.
     */
    Engine createNonSaslEngine();

}
