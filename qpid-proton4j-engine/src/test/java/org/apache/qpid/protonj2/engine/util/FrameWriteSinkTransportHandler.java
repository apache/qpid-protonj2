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
package org.apache.qpid.protonj2.engine.util;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.EngineHandler;
import org.apache.qpid.protonj2.engine.EngineHandlerContext;
import org.apache.qpid.protonj2.engine.HeaderFrame;
import org.apache.qpid.protonj2.engine.ProtocolFrame;
import org.apache.qpid.protonj2.engine.SaslFrame;
import org.apache.qpid.protonj2.types.security.SaslPerformative;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.apache.qpid.protonj2.types.transport.Performative;

/**
 * All writes are dropped to prevent errors in tests where no outbound capture happens.
 */
public class FrameWriteSinkTransportHandler implements EngineHandler {

    public FrameWriteSinkTransportHandler() {
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
    public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
    }

    @Override
    public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
    }

    @Override
    public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
    }
}
