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
package org.apache.qpid.proton4j.transport.impl;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.Frame;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.Transport;
import org.apache.qpid.proton4j.transport.TransportHandler;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;

/**
 * Context for a registered TransportHandler
 */
public class ProtonTransportHandlerContext implements TransportHandlerContext {

    ProtonTransportHandlerContext previous;
    ProtonTransportHandlerContext next;

    private final Transport transport;
    private final TransportHandler handler;

    public ProtonTransportHandlerContext(Transport transport, TransportHandler handler) {
        this.transport = transport;
        this.handler = handler;
    }

    TransportHandler getHandler() {
        return handler;
    }

    @Override
    public Transport getTransport() {
        return transport;
    }

    @Override
    public void fireRead(ProtonBuffer buffer) {
        previous.getHandler().handleRead(previous, buffer);
    }

    @Override
    public void fireHeaderFrame(HeaderFrame header) {
        previous.getHandler().handleHeaderFrame(previous, header);
    }

    @Override
    public void fireSaslFrame(SaslFrame frame) {
        previous.getHandler().handleSaslFrame(previous, frame);
    }

    @Override
    public void fireProtocolFrame(ProtocolFrame frame) {
        previous.getHandler().handleProtocolFrame(previous, frame);
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
    public void fireWrite(Frame<?> frame) {
        next.getHandler().handleWrite(next, frame);
    }

    @Override
    public void fireWrite(ProtonBuffer buffer) {
        next.getHandler().handleWrite(next, buffer);
    }

    @Override
    public void fireFlush() {
        next.getHandler().handleFlush(next);
    }
}
