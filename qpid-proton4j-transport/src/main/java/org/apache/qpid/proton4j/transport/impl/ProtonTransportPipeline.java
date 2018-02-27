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

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.Frame;
import org.apache.qpid.proton4j.transport.HeaderFrame;
import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.TransportHandler;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.TransportListener;
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

    //----- Event injection methods ------------------------------------------//

    @Override
    public TransportPipeline fireRead(ProtonBuffer input) {
        head.next.fireRead(input);
        return this;
    }

    @Override
    public TransportPipeline fireHeaderFrame(HeaderFrame header) {
        head.next.fireHeaderFrame(header);
        return this;
    }

    @Override
    public TransportPipeline fireSaslFrame(SaslFrame frame) {
        head.next.fireSaslFrame(frame);
        return this;
    }

    @Override
    public TransportPipeline fireProtocolFrame(ProtocolFrame frame) {
        head.next.fireProtocolFrame(frame);
        return this;
    }

    @Override
    public TransportPipeline fireWrite(Frame<?> frame) {
        tail.previous.fireWrite(frame);
        return this;
    }

    @Override
    public TransportPipeline fireFlush() {
        tail.previous.fireFlush();
        return this;
    }

    @Override
    public TransportPipeline fireEncodingError(Throwable e) {
        head.next.fireEncodingError(e);
        return this;
    }

    @Override
    public TransportPipeline fireDecodingError(Throwable e) {
        head.next.fireDecodingError(e);
        return this;
    }

    @Override
    public TransportPipeline fireFailed(Throwable e) {
        head.next.fireFailed(e);
        return this;
    }

    //----- Synthetic handler context that bounds the pipeline ---------------//

    private class TransportHandlerContextBoundry extends ProtonTransportHandlerContext {

        public TransportHandlerContextBoundry() {
            super(transport, null);
        }

        @Override
        public void fireRead(ProtonBuffer buffer) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed Transport read event."));
            }
        }

        @Override
        public void fireHeaderFrame(HeaderFrame header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed AMQP Header event."));
            }
        }

        @Override
        public void fireSaslFrame(SaslFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed SASL frame event."));
            }
        }

        @Override
        public void fireProtocolFrame(ProtocolFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed protocol frame event."));
            }
        }

        @Override
        public void fireEncodingError(Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed encoding error.", e));
            }
        }

        @Override
        public void fireDecodingError(Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed decoding error.", e));
            }
        }

        @Override
        public void fireFailed(Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport, e);
            }
        }

        @Override
        public void fireWrite(Frame<?> frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write frame event."));
            }
        }

        @Override
        public void fireWrite(ProtonBuffer buffer) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write data event."));
            }
        }

        @Override
        public void fireFlush() {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed flush event."));
            }
        }
    }
}
