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
import org.apache.qpid.proton4j.transport.TransportHandler;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.TransportPipeline;

/**
 * Pipeline of TransportHandlers used to process IO
 */
public class ProtonTransportPipeline implements TransportPipeline {

    TransportHandlerContextBoundry head;
    TransportHandlerContextBoundry tail;

    private final ProtonTransport transport;

    ProtonTransportPipeline(ProtonTransport transport) {
        if (transport == null) {
            throw new IllegalArgumentException("Parent transport cannot be null");
        }

        this.transport = transport;

        this.head = new TransportHandlerContextBoundry();
        this.tail = new TransportHandlerContextBoundry();

        // Ensure Pipeline starts out empty but initialized.
        this.head.next = this.tail;
        this.tail.previous = this.head;
    }

    @Override
    public ProtonTransport getTransport() {
        return transport;
    }

    @Override
    public TransportPipeline addFirst(String name, TransportHandler handler) {
        return null;
    }

    @Override
    public TransportPipeline addLast(String name, TransportHandler handler) {
        return null;
    }

    @Override
    public TransportPipeline removeFirst() {
        return null;
    }

    @Override
    public TransportPipeline removeLast() {
        return null;
    }

    @Override
    public TransportPipeline remove(String name) {
        return null;
    }

    @Override
    public TransportHandler first() {
        return null;
    }

    @Override
    public TransportHandler last() {
        return null;
    }

    @Override
    public TransportHandlerContext firstContext() {
        return null;
    }

    @Override
    public TransportHandlerContext lastContext() {
        return null;
    }

    //----- Synthetic handler context that bounds the pipeline ---------------//

    private class TransportHandlerContextBoundry extends ProtonTransportHandlerContext {

        public TransportHandlerContextBoundry() {
            super(null);
        }

        @Override
        public void fireRead(ProtonBuffer buffer) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireHeaderFrame(HeaderFrame header) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireSaslFrame(SaslFrame frame) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireProtocolFrame(ProtocolFrame frame) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireEncodingError(Throwable e) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireDecodingError(Throwable e) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireFailed(Throwable e) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireWrite(Frame<?> frame) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireWrite(ProtonBuffer buffer) {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }

        @Override
        public void fireFlush() {
            // TODO - Signal the Transport that no handler intercepted this and we should fail
        }
    }
}
