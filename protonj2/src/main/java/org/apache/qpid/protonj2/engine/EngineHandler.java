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
import org.apache.qpid.protonj2.types.transport.Performative;

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

    /**
     * Handle the read of new incoming bytes from a remote sender.  The handler should generally
     * decode these bytes into an AMQP Performative or SASL Performative based on the current state
     * of the connection and the handler in question.
     *
     * @param context
     *      The context for this handler which can be used to forward the event to the next handler
     * @param buffer
     *      The buffer containing the bytes that the engine handler should decode.
     */
    default void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
        context.fireRead(buffer);
    }

    /**
     * Handle the receipt of an incoming AMQP Header or SASL Header based on the current state
     * of this handler.
     *
     * @param context
     *      The context for this handler which can be used to forward the event to the next handler
     * @param header
     *      The AMQP Header envelope that wraps the received header instance.
     */
    default void handleRead(EngineHandlerContext context, HeaderEnvelope header) {
        context.fireRead(header);
    }

    /**
     * Handle the receipt of an incoming SASL performative based on the current state of this handler.
     *
     * @param context
     *      The context for this handler which can be used to forward the event to the next handler
     * @param envelope
     *      The SASL envelope that wraps the received {@link SaslPerformative}.
     */
    default void handleRead(EngineHandlerContext context, SASLEnvelope envelope) {
        context.fireRead(envelope);
    }

    /**
     * Handle the receipt of an incoming AMQP envelope based on the current state of this handler.
     *
     * @param context
     *      The context for this handler which can be used to forward the event to the next handler
     * @param envelope
     *      The AMQP envelope that wraps the received {@link Performative}.
     */
    default void handleRead(EngineHandlerContext context, IncomingAMQPEnvelope envelope) {
        context.fireRead(envelope);
    }

    /**
     * Handles write of AMQPHeader either by directly writing it to the output target or by
     * converting it to bytes and firing a write using the {@link ProtonBuffer} based API
     * in {@link EngineHandlerContext#fireWrite(ProtonBuffer)}
     *
     * @param context
     *      The {@link EngineHandlerContext} associated with this {@link EngineWriteHandler} instance.
     * @param envelope
     *      The {@link HeaderEnvelope} instance to write.
     */
    default void handleWrite(EngineHandlerContext context, HeaderEnvelope envelope) {
        context.fireWrite(envelope);
    }

    /**
     * Handles write of AMQP performative envelope either by directly writing it to the output target or
     * by converting it to bytes and firing a write using the {@link ProtonBuffer} based API in
     * {@link EngineHandlerContext#fireWrite(ProtonBuffer)}
     *
     * @param context
     *      The {@link EngineHandlerContext} associated with this {@link EngineWriteHandler} instance.
     * @param envelope
     *      The {@link OutgoingAMQPEnvelope} instance to write.
     */
    default void handleWrite(EngineHandlerContext context, OutgoingAMQPEnvelope envelope) {
        context.fireWrite(envelope);
    }

    /**
     * Handles write of SaslPerformative either by directly writing it to the output target or by
     * converting it to bytes and firing a write using the {@link ProtonBuffer} based API
     * in {@link EngineHandlerContext#fireWrite(ProtonBuffer)}
     *
     * @param context
     *      The {@link EngineHandlerContext} associated with this {@link EngineWriteHandler} instance.
     * @param envelope
     *      The {@link SASLEnvelope} instance to write.
     */
    default void handleWrite(EngineHandlerContext context, SASLEnvelope envelope) {
        context.fireWrite(envelope);
    }

    /**
     * Writes the given bytes to the output target or if no handler in the pipeline handles this
     * calls the registered output handler of the parent Engine instance.  If not output handler
     * is found or not handler in the output chain consumes this write the Engine will be failed
     * as an output sink is required for all low level engine writes.
     *
     * @param context
     *      The {@link EngineHandlerContext} associated with this {@link EngineWriteHandler} instance.
     * @param buffer
     *      The {@link ProtonBuffer} whose payload is to be written to the output target.
     */
    default void handleWrite(EngineHandlerContext context, ProtonBuffer buffer) {
        context.fireWrite(buffer);
    }
}
