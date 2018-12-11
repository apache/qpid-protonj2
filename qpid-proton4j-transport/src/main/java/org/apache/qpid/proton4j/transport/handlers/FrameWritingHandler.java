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
package org.apache.qpid.proton4j.transport.handlers;

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.Frame;
import org.apache.qpid.proton4j.transport.TransportHandlerAdapter;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;

/**
 * Handler that encodes performatives into properly formed frames for IO
 */
public class FrameWritingHandler extends TransportHandlerAdapter {

    // TODO - in order to enforce max frame size we need to have that value here
    //        but it can vary between the sasl and non-sasl layers.

    // TODO - Need access to the appropriate encoder here

    public FrameWritingHandler() {
    }

    @Override
    public void handleWrite(TransportHandlerContext context, AMQPHeader header) {
    }

    @Override
    public void handleWrite(TransportHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
    }

    @Override
    public void handleWrite(TransportHandlerContext context, SaslPerformative performative) {
    }

    @Override
    public void handleWrite(TransportHandlerContext context, Frame<?> frame) {
    }
}
