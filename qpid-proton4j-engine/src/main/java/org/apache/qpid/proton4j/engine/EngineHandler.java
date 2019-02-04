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

    default void handlerAdded(EngineHandlerContext context) throws Exception {}

    default void handlerRemoved(EngineHandlerContext context) throws Exception {}

    // Some things that might flow through an engine pipeline

    // Give handlers a chance to set initial state prior to start using fixed engine configuration
    // void engineStarting(EngineSaslContext context) throws Exception;

    // Read events

    // void handleReadabilityChanged(TransportHandlerContext context);  // TODO how to communicate readable state ?

    default void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
        context.fireRead(buffer);
    }

    default void handleRead(EngineHandlerContext context, HeaderFrame header) {
        context.fireRead(header);
    }

    default void handleRead(EngineHandlerContext context, SaslFrame frame) {
        context.fireRead(frame);
    }

    default void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        context.fireRead(frame);
    }

    // Write events

    // void handleWritabilityChanged(TransportHandlerContext context); // TODO how to communicate writable state ?

    default void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        context.fireWrite(header);
    }

    default void handleWrite(EngineHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
        context.fireWrite(performative, channel, payload, payloadToLarge);
    }

    default void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        context.fireWrite(performative);
    }

    default void handleWrite(EngineHandlerContext context, ProtonBuffer buffer) {
        context.fireWrite(buffer);
    }

    // Error events

    default void transportEncodingError(EngineHandlerContext context, Throwable e) {
        context.fireEncodingError(e);
    }

    default void transportDecodingError(EngineHandlerContext context, Throwable e) {
        context.fireDecodingError(e);
    }

    default void transportFailed(EngineHandlerContext context, Throwable e) {
        context.fireFailed(e);
    }
}
