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
package org.apache.qpid.protonj2.engine.impl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.HeaderEnvelope;
import org.apache.qpid.protonj2.engine.IncomingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.OutgoingAMQPEnvelope;
import org.apache.qpid.protonj2.engine.SASLEnvelope;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;

/**
 * Context for a registered EngineHandler
 */
public class ProtonEngineHandlerContext implements EngineHandlerContext {

    ProtonEngineHandlerContext previous;
    ProtonEngineHandlerContext next;

    /**
     * The context indicator for a handler that wants to be sent read events.
     */
    public static final int HANDLER_READS = 1 << 1;

    /**
     * The context indicator for a handler that wants to be sent write events.
     */
    public static final int HANDLER_WRITES = 1 << 2;

    /**
     * The context indicator for a handler that wants to be sent all read and write events.
     */
    public static final int HANDLER_ALL_EVENTS = HANDLER_READS | HANDLER_WRITES;

    private final String name;
    private final Engine engine;
    private final EngineHandler handler;

    private int interestMask = HANDLER_ALL_EVENTS;

    /**
     * Creates a new {@link ProtonEngineHandlerContext} with the given options.
     *
     * @param name
     * 		The name of this {@link ProtonEngineHandlerContext}.
     * @param engine
     * 		The engine that this context is assigned to.
     * @param handler
     * 		The {@link EngineHandler} that this {@link EngineHandlerContext} manages.
     */
    public ProtonEngineHandlerContext(String name, Engine engine, EngineHandler handler) {
        this.name = name;
        this.engine = engine;
        this.handler = handler;
    }

    @Override
    public EngineHandler handler() {
        return handler;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Engine engine() {
        return engine;
    }

    /**
     * Allows a handler to indicate if it wants to be notified of a Engine Handler events for
     * specific operations or opt into all engine handler events.  By opting out of the events
     * that the handler does not process the call chain can be reduced when processing engine
     * events.
     *
     * @return the interest mask that should be used to determine if a handler should be signaled.
     */
    public int interestMask() {
        return interestMask;
    }

    /**
     * Sets the interest mask for this handler context which controls which events should be routed
     * here during normal engine handler pipeline operations.
     *
     * @param mask
     * 		The interest mask for this {@link EngineHandlerContext}.
     *
     * @return this {@link ProtonEngineHandlerContext} instance.
     */
    public ProtonEngineHandlerContext interestMask(int mask) {
        this.interestMask = mask;
        return this;
    }

    @Override
    public void fireEngineStarting() {
        next.invokeEngineStarting();
    }

    @Override
    public void fireEngineStateChanged() {
        next.invokeEngineStateChanged();
    }

    @Override
    public void fireFailed(EngineFailedException failure) {
        next.invokeEngineFailed(failure);
    }

    @Override
    public void fireRead(ProtonBuffer buffer) {
        findNextReadHandler().invokeHandlerRead(buffer);
    }

    @Override
    public void fireRead(HeaderEnvelope header) {
        findNextReadHandler().invokeHandlerRead(header);
    }

    @Override
    public void fireRead(SASLEnvelope envelope) {
        findNextReadHandler().invokeHandlerRead(envelope);
    }

    @Override
    public void fireRead(IncomingAMQPEnvelope envelope) {
        findNextReadHandler().invokeHandlerRead(envelope);
    }

    @Override
    public void fireWrite(OutgoingAMQPEnvelope envelope) {
        findNextWriteHandler().invokeHandlerWrite(envelope);
    }

    @Override
    public void fireWrite(SASLEnvelope envelope) {
        findNextWriteHandler().invokeHandlerWrite(envelope);
    }

    @Override
    public void fireWrite(HeaderEnvelope envelope) {
        findNextWriteHandler().invokeHandlerWrite(envelope);
    }

    @Override
    public void fireWrite(ProtonBuffer buffer, Runnable ioComplete) {
        findNextWriteHandler().invokeHandlerWrite(buffer, ioComplete);
    }

    //----- Internal invoke of Engine and Handler state methods

    void invokeEngineStarting() {
        handler.engineStarting(this);
    }

    void invokeEngineStateChanged() {
        handler.handleEngineStateChanged(this);
    }

    void invokeEngineFailed(EngineFailedException failure) {
        handler.engineFailed(this, failure);
    }

    //----- Internal invoke of Read methods

    void invokeHandlerRead(IncomingAMQPEnvelope envelope) {
        handler.handleRead(this, envelope);
    }

    void invokeHandlerRead(SASLEnvelope envelope) {
        handler.handleRead(this, envelope);
    }

    void invokeHandlerRead(HeaderEnvelope envelope) {
        handler.handleRead(this, envelope);
    }

    void invokeHandlerRead(ProtonBuffer buffer) {
        handler.handleRead(this, buffer);
    }

    //----- Internal invoke of Write methods

    void invokeHandlerWrite(OutgoingAMQPEnvelope envelope) {
        handler.handleWrite(this, envelope);
    }

    void invokeHandlerWrite(SASLEnvelope envelope) {
        handler.handleWrite(this, envelope);
    }

    void invokeHandlerWrite(HeaderEnvelope envelope) {
        handler.handleWrite(this, envelope);
    }

    void invokeHandlerWrite(ProtonBuffer buffer, Runnable ioComplete) {
        next.handler().handleWrite(next, buffer, ioComplete);
    }

    private ProtonEngineHandlerContext findNextReadHandler() {
        ProtonEngineHandlerContext ctx = this;
        do {
            ctx = ctx.previous;
        } while (skipContext(ctx, HANDLER_READS));
        return ctx;
    }

    private ProtonEngineHandlerContext findNextWriteHandler() {
        ProtonEngineHandlerContext ctx = this;
        do {
            ctx = ctx.next;
        } while (skipContext(ctx, HANDLER_WRITES));
        return ctx;
    }

    private static boolean skipContext(ProtonEngineHandlerContext ctx, int interestMask) {
        return (ctx.interestMask() & interestMask) == 0;
    }
}
