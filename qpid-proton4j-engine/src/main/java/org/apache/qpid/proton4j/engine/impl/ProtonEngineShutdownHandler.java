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
import org.apache.qpid.proton4j.engine.exceptions.EngineFailedException;
import org.apache.qpid.proton4j.engine.exceptions.EngineShutdownException;

/**
 * Handler used to guard the {@link ProtonEngine} pipeline from writes or reads initiated
 * after the engine has been shutdown either normally or due to some failure.
 */
public class ProtonEngineShutdownHandler implements EngineHandler {

    public static final ProtonEngineShutdownHandler INSTANCE = new ProtonEngineShutdownHandler();

    private void fireShutdownError(EngineHandlerContext context, String message) {
        if (context.getEngine().isFailed()) {
            throw new EngineFailedException(message, context.getEngine().failureCause());
        } else {
            throw new EngineShutdownException(message);
        }
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
        fireShutdownError(context, "Engine cannot read new incoming data when shutdown");
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
        fireShutdownError(context, "Engine cannot read new incoming AMQP Header when shutdown");
    }

    @Override
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        fireShutdownError(context, "Engine cannot read new incoming SASL frame when shutdown");
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        fireShutdownError(context, "Engine cannot read new incoming AMQP Frame when shutdown");
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        fireShutdownError(context, "Engine cannot write AMQP Header after it has been shutdown");
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        fireShutdownError(context, "Engine cannot write AMQP Performative after it has been shutdown");
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        fireShutdownError(context, "Engine cannot write SASL Performative after it has been shutdown");
    }

    @Override
    public void handleWrite(EngineHandlerContext context, ProtonBuffer buffer) {
        fireShutdownError(context, "Engine cannot write data after it has been shutdown");
    }
}
