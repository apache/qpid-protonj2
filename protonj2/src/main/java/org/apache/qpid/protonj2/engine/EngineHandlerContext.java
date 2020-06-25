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
import org.apache.qpid.protonj2.types.security.SaslPerformative;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.Performative;

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

    void fireEngineStarting();

    void fireEngineStateChanged();

    void fireFailed(EngineFailedException failure);

    void fireRead(ProtonBuffer buffer);

    void fireRead(HeaderFrame header);

    void fireRead(SaslFrame frame);

    void fireRead(ProtocolFrame frame);

    void fireWrite(AMQPHeader header);

    void fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge);

    void fireWrite(SaslPerformative performative);

    void fireWrite(ProtonBuffer buffer);

}
