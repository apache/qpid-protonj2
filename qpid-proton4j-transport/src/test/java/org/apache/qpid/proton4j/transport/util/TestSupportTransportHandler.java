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
package org.apache.qpid.proton4j.transport.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.Frame;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.TransportHandler;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;

public class TestSupportTransportHandler implements TransportHandler{

    private List<Frame<?>> framesRead = new ArrayList<>();
    private List<Frame<?>> framesWritten = new ArrayList<>();

    public TestSupportTransportHandler() {
    }

    public List<Frame<?>> getFramesWritten() {
        return framesWritten;
    }

    public List<Frame<?>> getFramesRead() {
        return framesRead;
    }

    @Override
    public void handleRead(TransportHandlerContext context, ProtonBuffer buffer) {
        context.fireRead(buffer);
    }

    @Override
    public void handleHeaderFrame(TransportHandlerContext context, HeaderFrame header) {
        framesRead.add(header);
        context.fireHeaderFrame(header);
    }

    @Override
    public void handleSaslFrame(TransportHandlerContext context, SaslFrame frame) {
        framesRead.add(frame);
        context.fireSaslFrame(frame);
    }

    @Override
    public void handleProtocolFrame(TransportHandlerContext context, ProtocolFrame frame) {
        framesRead.add(frame);
        context.fireProtocolFrame(frame);
    }

    @Override
    public void transportEncodingError(TransportHandlerContext context, Throwable e) {
        context.fireEncodingError(e);
    }

    @Override
    public void transportDecodingError(TransportHandlerContext context, Throwable e) {
        context.fireDecodingError(e);
    }

    @Override
    public void transportFailed(TransportHandlerContext context, Throwable e) {
        context.fireFailed(e);
    }

    @Override
    public void handleWrite(TransportHandlerContext context, Frame<?> frame) {
        framesWritten.add(frame);
        context.fireWrite(frame);
    }

    @Override
    public void handleWrite(TransportHandlerContext context, AMQPHeader header) {
        context.fireWrite(header);
    }

    @Override
    public void handleWrite(TransportHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
        context.fireWrite(performative, channel, payload, payloadToLarge);
    }

    @Override
    public void handleWrite(TransportHandlerContext context, SaslPerformative performative) {
        context.fireWrite(performative);
    }

    @Override
    public void handleWrite(TransportHandlerContext context, ProtonBuffer buffer) {
        context.fireWrite(buffer);
    }

    @Override
    public void handleFlush(TransportHandlerContext context) {
        context.fireFlush();
    }
}
