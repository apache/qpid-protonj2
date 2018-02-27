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
package org.apache.qpid.proton4j.transport;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Simple abstract TransportHandler stub use by subclasses of {@link TransportHandler} that
 * forwards all calls not implemented in the subclass onto the next handler in the chain.
 */
public abstract class TransportHandlerAdapter implements TransportHandler {

    @Override
    public void handleRead(TransportHandlerContext context, ProtonBuffer buffer) {
        context.fireRead(buffer);
    }

    @Override
    public void handleHeaderFrame(TransportHandlerContext context, HeaderFrame header) {
        context.fireHeaderFrame(header);
    }

    @Override
    public void handleSaslFrame(TransportHandlerContext context, SaslFrame frame) {
        context.fireSaslFrame(frame);
    }

    @Override
    public void handleProtocolFrame(TransportHandlerContext context, ProtocolFrame frame) {
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
        context.fireWrite(frame);
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
