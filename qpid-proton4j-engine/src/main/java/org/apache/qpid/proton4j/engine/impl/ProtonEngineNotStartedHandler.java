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
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.SaslFrame;
import org.apache.qpid.proton4j.engine.exceptions.EngineNotStartedException;

/**
 * Handler used to guard the {@link ProtonEngine} pipeline from writes initiated
 * before the engine has been properly started.
 */
public class ProtonEngineNotStartedHandler implements EngineHandler {

    public static final ProtonEngineNotStartedHandler INSTANCE = new ProtonEngineNotStartedHandler();

    @Override
    public void engineStarting(EngineHandlerContext context) {
        context.getEngine().pipeline().remove(this);
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
        throw new EngineNotStartedException("Not started Proton Engine cannot ingest data.");
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
        throw new EngineNotStartedException("Not started Proton Engine cannot ingest AMQP Headers.");
    }

    @Override
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        throw new EngineNotStartedException("Not started Proton Engine cannot ingest SASL Frames.");
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        throw new EngineNotStartedException("Not started Proton Engine cannot AMQP Performatives.");
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        throw new EngineNotStartedException("Not started Proton Engine cannot write AMQP Headers.");
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        throw new EngineNotStartedException("Not started Proton Engine cannot write AMQP Performatives.");
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        throw new EngineNotStartedException("Not started Proton Engine cannot write SASL Performatives.");
    }

    @Override
    public void handleWrite(EngineHandlerContext context, ProtonBuffer buffer) {
        throw new EngineNotStartedException("Not started Proton Engine cannot write data.");
    }
}
