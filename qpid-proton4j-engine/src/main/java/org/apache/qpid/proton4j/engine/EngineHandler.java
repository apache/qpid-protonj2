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
 * Listen for events generated from the Engine
 */
public interface EngineHandler {

    // Life cycle events for a handler

    void handlerAdded(EngineHandlerContext context) throws Exception;

    void handlerRemoved(EngineHandlerContext context) throws Exception;

    // Some things that might flow through an engine pipeline

    // Give handlers a chance to set initial state prior to start using fixed engine configuration
    // void engineStarting(EngineSaslContext context) throws Exception;

    // Read events

    // void handleReadabilityChanged(TransportHandlerContext context);  // TODO how to communicate readable state ?

    void handleRead(EngineHandlerContext context, ProtonBuffer buffer);

    void handleRead(EngineHandlerContext context, HeaderFrame header);

    void handleRead(EngineHandlerContext context, SaslFrame frame);

    void handleRead(EngineHandlerContext context, ProtocolFrame frame);

    // Write events

    // void handleWritabilityChanged(TransportHandlerContext context); // TODO how to communicate writable state ?

    void handleWrite(EngineHandlerContext context, AMQPHeader header);

    void handleWrite(EngineHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge);

    void handleWrite(EngineHandlerContext context, SaslPerformative performative);

    void handleWrite(EngineHandlerContext context, ProtonBuffer buffer);

    // Error events

    void transportEncodingError(EngineHandlerContext context, Throwable e);

    void transportDecodingError(EngineHandlerContext context, Throwable e);

    void transportFailed(EngineHandlerContext context, Throwable e);
}
