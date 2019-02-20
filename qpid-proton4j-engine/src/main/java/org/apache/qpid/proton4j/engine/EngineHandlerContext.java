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

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Context provided to EngineHandler events to allow further event propagation
 */
public interface EngineHandlerContext {

    EngineHandler getHandler();

    Engine getEngine();

    String getName();

    void fireEngineStarting();

    void fireEngineStateChanged();

    void fireRead(ProtonBuffer buffer);

    void fireRead(HeaderFrame header);

    void fireRead(SaslFrame frame);

    void fireRead(ProtocolFrame frame);

    void fireWrite(AMQPHeader header);

    void fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge);

    void fireWrite(SaslPerformative performative);

    void fireWrite(ProtonBuffer buffer);

    void fireEncodingError(Throwable e);

    void fireDecodingError(Throwable e);

    void fireFailed(Throwable e);

}
