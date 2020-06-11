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
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.types.security.SaslPerformative;
import org.apache.qpid.proton4j.types.transport.AMQPHeader;
import org.apache.qpid.proton4j.types.transport.Performative;

/**
 * Listen for events generated from the Engine
 */
public interface EngineHandler {

    /**
     * Called when the handler is successfully added to the {@link EnginePipeline} and
     * will later be initialized before use.
     *
     * @param context
     *      The context that is assigned to this handler.
     */
    default void handlerAdded(EngineHandlerContext context) {}

    /**
     * Called when the handler is successfully removed to the {@link EnginePipeline}.
     *
     * @param context
     *      The context that is assigned to this handler.
     */
    default void handlerRemoved(EngineHandlerContext context) {}

    /**
     * Called when the engine is started to allow handlers to prepare for use based on
     * the configuration state at start of the engine.  A handler can fail the engine start
     * by throwing an exception.
     *
     * @param context
     *      The context for this handler which can be used to forward the event to the next handler
     */
    default void engineStarting(EngineHandlerContext context) {}

    /**
     * Called when the engine state has changed and handlers may need to update their internal state
     * to respond to the change or prompt some new work based on the change, e.g state changes from
     * not writable to writable.
     *
     * @param context
     *      The context for this handler which can be used to forward the event to the next handler
     */
    default void handleEngineStateChanged(EngineHandlerContext context) {
        context.fireEngineStateChanged();
    }

    /**
     * Called when the engine has transitioned to a failed state and cannot process any additional
     * input or output.  The handler can free and resources used for normal operations at this point
     * as the engine is now considered shutdown.
     *
     * @param context
     *      The context for this handler which can be used to forward the event to the next handler
     * @param failure
     *      The failure that triggered the engine to cease operations.
     */
    default void engineFailed(EngineHandlerContext context, EngineFailedException failure) {
        context.fireFailed(failure);
    }

    // Read events

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

    default void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        context.fireWrite(header);
    }

    default void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        context.fireWrite(performative, channel, payload, payloadToLarge);
    }

    default void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        context.fireWrite(performative);
    }

    default void handleWrite(EngineHandlerContext context, ProtonBuffer buffer) {
        context.fireWrite(buffer);
    }
}
