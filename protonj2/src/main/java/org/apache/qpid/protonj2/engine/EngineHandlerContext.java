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

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;

/**
 * Context provided to EngineHandler events to allow further event propagation
 */
public interface EngineHandlerContext {

    /**
     * @return the {@link EngineHandler} that is associated with the context.
     */
    EngineHandler handler();

    /**
     * @return the {@link Engine} where this handler is registered.
     */
    Engine engine();

    /**
     * @return the name that assigned to this {@link EngineHandler} when added to the {@link EnginePipeline}.
     */
    String name();

    /**
     * Fires the engine starting event into the next handler in the {@link EnginePipeline} chain.
     */
    void fireEngineStarting();

    /**
     * Fires the engine state changed event into the next handler in the {@link EnginePipeline} chain.  The
     * state change events occur after the engine starting event and generally signify that the engine has been
     * shutdown normally.
     */
    void fireEngineStateChanged();

    /**
     * Fires the {@link Engine} failed event into the next handler in the {@link EnginePipeline} chain.
     *
     * @param failure
     *      The exception that describes the conditions under which the engine failed.
     */
    void fireFailed(EngineFailedException failure);

    /**
     * Fires a read of ProtonBuffer events into the previous handler in the {@link EnginePipeline} for further
     * processing.
     *
     * @param buffer
     *      The {@link ProtonBuffer} that carries the bytes read.
     */
    void fireRead(ProtonBuffer buffer);

    /**
     * Fires a read of HeaderFrame events into the previous handler in the {@link EnginePipeline} for further
     * processing.
     *
     * @param header
     *      The {@link HeaderFrame} that carries the header bytes read.
     */
    void fireRead(HeaderFrame header);

    /**
     * Fires a read of SaslFrame events into the previous handler in the {@link EnginePipeline} for further
     * processing.
     *
     * @param frame
     *      The {@link SaslFrame} that carries the SASL performative read.
     */
    void fireRead(SaslFrame frame);

    /**
     * Fires a read of IncomingProtocolFrame events into the previous handler in the {@link EnginePipeline} for further
     * processing.
     *
     * @param frame
     *      The {@link IncomingProtocolFrame} that carries the AMQP performative read.
     */
    void fireRead(IncomingProtocolFrame frame);

    /**
     * Fires a write of {@link OutgoingProtocolFrame} events into the next handler in the {@link EnginePipeline} for further
     * processing.
     *
     * @param frame
     *      The {@link OutgoingProtocolFrame} that carries the AMQP performative being written.
     */
    void fireWrite(OutgoingProtocolFrame frame);

    /**
     * Fires a write of {@link SaslFrame} events into the next handler in the {@link EnginePipeline} for further
     * processing.
     *
     * @param frame
     *      The {@link SaslFrame} that carries the SASL performative being written.
     */
    void fireWrite(SaslFrame frame);

    /**
     * Fires a write of HeaderFrame events into the next handler in the {@link EnginePipeline} for further
     * processing.
     *
     * @param frame
     *      The {@link HeaderFrame} that carries the AMQP Header being written.
     */
    void fireWrite(HeaderFrame frame);

    /**
     * Fires a write of ProtonBuffer events into the next handler in the {@link EnginePipeline} for further
     * processing.
     *
     * @param buffer
     *      The {@link ProtonBuffer} that carries the bytes being written.
     */
    void fireWrite(ProtonBuffer buffer);

}
