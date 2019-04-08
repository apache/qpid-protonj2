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
 * Pipeline of handlers for Engine work.
 */
public interface EnginePipeline {

    Engine engine();

    //----- Pipeline management ----------------------------------------------//

    EnginePipeline addFirst(String name, EngineHandler handler);

    EnginePipeline addLast(String name, EngineHandler handler);

    EnginePipeline removeFirst();

    EnginePipeline removeLast();

    EnginePipeline remove(String name);

    EngineHandler first();

    EngineHandler last();

    EngineHandlerContext firstContext();

    EngineHandlerContext lastContext();

    //----- Event triggers ---------------------------------------------------//

    EnginePipeline fireEngineStarting();

    EnginePipeline fireEngineStateChanged();

    EnginePipeline fireRead(ProtonBuffer input);

    EnginePipeline fireRead(HeaderFrame header);

    EnginePipeline fireRead(SaslFrame frame);

    EnginePipeline fireRead(ProtocolFrame frame);

    EnginePipeline fireWrite(AMQPHeader header);

    EnginePipeline fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge);

    EnginePipeline fireWrite(SaslPerformative performative);

    EnginePipeline fireWrite(ProtonBuffer buffer);

    EnginePipeline fireEncodingError(Throwable e);

    EnginePipeline fireDecodingError(Throwable e);

    EnginePipeline fireFailed(Throwable e);

}
