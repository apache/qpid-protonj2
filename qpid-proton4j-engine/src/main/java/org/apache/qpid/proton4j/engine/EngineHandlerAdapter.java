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
 * Simple abstract TransportHandler stub use by subclasses of {@link EngineHandler} that
 * forwards all calls not implemented in the subclass onto the next handler in the chain.
 */
public abstract class EngineHandlerAdapter implements EngineHandler {

    @Override
    public void handlerAdded(EngineHandlerContext context) throws Exception {
    }

    @Override
    public void handlerRemoved(EngineHandlerContext context) throws Exception {
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
        context.fireRead(buffer);
    }

    @Override
    public void handleRead(EngineHandlerContext context, HeaderFrame header) {
        context.fireRead(header);
    }

    @Override
    public void handleRead(EngineHandlerContext context, SaslFrame frame) {
        context.fireRead(frame);
    }

    @Override
    public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
        context.fireRead(frame);
    }

    @Override
    public void transportEncodingError(EngineHandlerContext context, Throwable e) {
        context.fireEncodingError(e);
    }

    @Override
    public void transportDecodingError(EngineHandlerContext context, Throwable e) {
        context.fireDecodingError(e);
    }

    @Override
    public void transportFailed(EngineHandlerContext context, Throwable e) {
        context.fireFailed(e);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
        context.fireWrite(header);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
        context.fireWrite(performative, channel, payload, payloadToLarge);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
        context.fireWrite(performative);
    }

    @Override
    public void handleWrite(EngineHandlerContext context, ProtonBuffer buffer) {
        context.fireWrite(buffer);
    }
}
