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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.apache.qpid.proton4j.transport.Frame;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.TransportHandler;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;

/**
 * Handler that will log incoming and outgoing Frames
 */
public class FrameLoggingHandler implements TransportHandler {

    private static ProtonLogger LOG = ProtonLoggerFactory.getLogger(FrameLoggingHandler.class);

    @Override
    public void handleRead(TransportHandlerContext context, ProtonBuffer buffer) {
    }

    @Override
    public void handleHeaderFrame(TransportHandlerContext context, HeaderFrame header) {
        LOG.trace("<- Header: {}", header);
        context.fireHeaderFrame(header);
    }

    @Override
    public void handleSaslFrame(TransportHandlerContext context, SaslFrame frame) {
        LOG.trace("<- SASL: {}", frame);
        context.fireSaslFrame(frame);
    }

    @Override
    public void handleProtocolFrame(TransportHandlerContext context, ProtocolFrame frame) {
        LOG.trace("<- AMQP: {}", frame);
        context.fireProtocolFrame(frame);
    }

    @Override
    public void transportEncodingError(TransportHandlerContext context, Throwable e) {
        // TODO
        context.fireEncodingError(e);
    }

    @Override
    public void transportDecodingError(TransportHandlerContext context, Throwable e) {
        // TODO
        context.fireDecodingError(e);
    }

    @Override
    public void transportFailed(TransportHandlerContext context, Throwable e) {
        // TODO
        context.fireFailed(e);
    }

    @Override
    public void handleWrite(TransportHandlerContext context, Frame<?> frame) {
        LOG.trace("-> Frame: {}", frame);
        context.fireWrite(frame);
    }

    @Override
    public void handleWrite(TransportHandlerContext context, ProtonBuffer buffer) {
        // TODO
        context.fireWrite(buffer);
    }

    @Override
    public void handleFlush(TransportHandlerContext context) {
        // TODO
        context.fireFlush();
    }
}
