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
import org.apache.qpid.protonj2.engine.HeaderFrame;
import org.apache.qpid.protonj2.engine.ProtocolFrame;
import org.apache.qpid.protonj2.engine.SaslFrame;
import org.apache.qpid.protonj2.engine.exceptions.EngineFailedException;
import org.apache.qpid.protonj2.types.security.SaslPerformative;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.Performative;

/**
 * Context for a registered EngineHandler
 */
public class ProtonEngineHandlerContext implements EngineHandlerContext {

    ProtonEngineHandlerContext previous;
    ProtonEngineHandlerContext next;

    private final String name;
    private final Engine engine;
    private final EngineHandler handler;

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

    @Override
    public void fireEngineStarting() {
        next.handler().engineStarting(next);
    }

    @Override
    public void fireEngineStateChanged() {
        next.handler().handleEngineStateChanged(next);
    }

    @Override
    public void fireRead(ProtonBuffer buffer) {
        previous.handler().handleRead(previous, buffer);
    }

    @Override
    public void fireRead(HeaderFrame header) {
        previous.handler().handleRead(previous, header);
    }

    @Override
    public void fireRead(SaslFrame frame) {
        previous.handler().handleRead(previous, frame);
    }

    @Override
    public void fireRead(ProtocolFrame frame) {
        previous.handler().handleRead(previous, frame);
    }

    @Override
    public void fireFailed(EngineFailedException failure) {
        next.handler().engineFailed(previous, failure);
    }

    @Override
    public void fireWrite(AMQPHeader header) {
        next.handler().handleWrite(next, header);
    }

    @Override
    public void fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        next.handler().handleWrite(next, performative, channel, payload, payloadToLarge);
    }

    @Override
    public void fireWrite(SaslPerformative performative) {
        next.handler().handleWrite(next, performative);
    }

    @Override
    public void fireWrite(ProtonBuffer buffer) {
        next.handler().handleWrite(next, buffer);
    }
}
