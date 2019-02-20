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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.SaslFrame;

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
    public EngineHandler getHandler() {
        return handler;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Engine getEngine() {
        return engine;
    }

    @Override
    public void fireEngineStarting() {
        next.getHandler().engineStarting(next);
    }

    @Override
    public void fireEngineStateChanged() {
        next.getHandler().handleEngineStateChanged(next);
    }

    @Override
    public void fireRead(ProtonBuffer buffer) {
        previous.getHandler().handleRead(previous, buffer);
    }

    @Override
    public void fireRead(HeaderFrame header) {
        previous.getHandler().handleRead(previous, header);
    }

    @Override
    public void fireRead(SaslFrame frame) {
        previous.getHandler().handleRead(previous, frame);
    }

    @Override
    public void fireRead(ProtocolFrame frame) {
        previous.getHandler().handleRead(previous, frame);
    }

    @Override
    public void fireEncodingError(Throwable e) {
        previous.getHandler().transportEncodingError(previous, e);
    }

    @Override
    public void fireDecodingError(Throwable e) {
        previous.getHandler().transportDecodingError(previous, e);
    }

    @Override
    public void fireFailed(Throwable e) {
        previous.getHandler().transportFailed(previous, e);
    }

    @Override
    public void fireWrite(AMQPHeader header) {
        next.getHandler().handleWrite(next, header);
    }

    @Override
    public void fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        next.getHandler().handleWrite(next, performative, channel, payload, payloadToLarge);
    }

    @Override
    public void fireWrite(SaslPerformative performative) {
        next.getHandler().handleWrite(next, performative);
    }

    @Override
    public void fireWrite(ProtonBuffer buffer) {
        next.getHandler().handleWrite(next, buffer);
    }
}
