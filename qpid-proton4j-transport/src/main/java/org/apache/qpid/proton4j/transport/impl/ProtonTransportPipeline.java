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
import org.apache.qpid.proton4j.transport.TransportListener;
import org.apache.qpid.proton4j.transport.TransportPipeline;

/**
 * Pipeline of TransportHandlers used to process IO
 */
public class ProtonTransportPipeline implements TransportPipeline {

    TransportHandlerContextReadBoundry head;
    TransportHandlerContextWriteBoundry tail;

    private final ProtonTransport transport;

    ProtonTransportPipeline(ProtonTransport transport) {
        if (transport == null) {
            throw new IllegalArgumentException("Parent transport cannot be null");
        }

        this.transport = transport;

        head = new TransportHandlerContextReadBoundry();
        tail = new TransportHandlerContextWriteBoundry();

        // Ensure Pipeline starts out empty but initialized.
        head.next = tail;
        head.previous = head;

        tail.previous = head;
        tail.next = tail;
    }

    @Override
    public ProtonTransport getTransport() {
        return transport;
    }

    @Override
    public TransportPipeline addFirst(String name, TransportHandler handler) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Handler name cannot be null or empty");
        }

        if (handler == null) {
            throw new IllegalArgumentException("Handler provided cannot be null");
        }

        ProtonTransportHandlerContext oldFirst = head.next;
        ProtonTransportHandlerContext newFirst = createContext(name, handler);

        newFirst.next = oldFirst;
        newFirst.previous = head;

        oldFirst.previous = newFirst;
        head.next = newFirst;

        return this;
    }

    @Override
    public TransportPipeline addLast(String name, TransportHandler handler) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Handler name cannot be null or empty");
        }

        if (handler == null) {
            throw new IllegalArgumentException("Handler provided cannot be null");
        }

        ProtonTransportHandlerContext oldLast = tail.previous;
        ProtonTransportHandlerContext newLast = createContext(name, handler);

        newLast.next = tail;
        newLast.previous = oldLast;

        oldLast.next = newLast;
        tail.previous = newLast;

        return this;
    }

    @Override
    public TransportPipeline removeFirst() {
        if (head.next != tail) {
            ProtonTransportHandlerContext oldFirst = head.next;

            head.next = oldFirst.next;
            head.next.previous = head;
        }

        return this;
    }

    @Override
    public TransportPipeline removeLast() {
        if (tail.previous != head) {
            ProtonTransportHandlerContext oldLast = tail.previous;

            tail.previous = oldLast.previous;
            tail.previous.next = tail;
        }

        return this;
    }

    @Override
    public TransportPipeline remove(String name) {
        if (name != null && !name.isEmpty()) {
            ProtonTransportHandlerContext current = head.next;
            while (current != tail) {
                if (current.getName().equals(name)) {
                    ProtonTransportHandlerContext newNext = current.next;

                    current.previous.next = newNext;
                    newNext.previous = current.previous;

                    break;
                }

                current = current.next;
            }
        }

        return this;
    }

    @Override
    public TransportHandler first() {
        return head.next == tail ? null : head.next.getHandler();
    }

    @Override
    public TransportHandler last() {
        return tail.previous == head ? null : tail.previous.getHandler();
    }

    @Override
    public TransportHandlerContext firstContext() {
        return head.next == tail ? null : head.next;
    }

    @Override
    public TransportHandlerContext lastContext() {
        return tail.previous == head ? null : tail.previous;
    }

    //----- Event injection methods ------------------------------------------//

    @Override
    public TransportPipeline fireRead(ProtonBuffer input) {
        tail.fireRead(input);
        return this;
    }

    @Override
    public TransportPipeline fireRead(HeaderFrame header) {
        tail.fireRead(header);
        return this;
    }

    @Override
    public TransportPipeline fireRead(SaslFrame frame) {
        tail.fireRead(frame);
        return this;
    }

    @Override
    public TransportPipeline fireRead(ProtocolFrame frame) {
        tail.fireRead(frame);
        return this;
    }

    @Override
    public TransportPipeline fireWrite(AMQPHeader header) {
        head.fireWrite(header);
        return this;
    }

    @Override
    public TransportPipeline fireWrite(Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
        head.fireWrite(performative, channel, payload, payloadToLarge);
        return this;
    }

    @Override
    public TransportPipeline fireWrite(SaslPerformative performative) {
        head.fireWrite(performative);
        return this;
    }

    @Override
    public TransportPipeline fireWrite(ProtonBuffer buffer) {
        head.fireWrite(buffer);
        return this;
    }

    @Override
    public TransportPipeline fireFlush() {
        head.fireFlush();
        return this;
    }

    @Override
    public TransportPipeline fireEncodingError(Throwable e) {
        tail.fireEncodingError(e);
        return this;
    }

    @Override
    public TransportPipeline fireDecodingError(Throwable e) {
        tail.fireDecodingError(e);
        return this;
    }

    @Override
    public TransportPipeline fireFailed(Throwable e) {
        tail.fireFailed(e);
        return this;
    }

    //----- Internal implementation ------------------------------------------//

    private ProtonTransportHandlerContext createContext(String name, TransportHandler handler) {
        return new ProtonTransportHandlerContext(name, transport, handler);
    }

    //----- Synthetic handler context that bounds the pipeline ---------------//

    private class TransportHandlerContextReadBoundry extends ProtonTransportHandlerContext {

        public TransportHandlerContextReadBoundry() {
            super("Read Boundry", transport, new BoundryTransportHandler());
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
        public void fireRead(HeaderFrame header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed AMQP Header event."));
            }
        }

        @Override
        public void fireRead(SaslFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed SASL frame event."));
            }
        }

        @Override
        public void fireRead(ProtocolFrame frame) {
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
    }

    private class TransportHandlerContextWriteBoundry extends ProtonTransportHandlerContext {

        public TransportHandlerContextWriteBoundry() {
            super("Write Boundry", transport, new BoundryTransportHandler());
        }

        @Override
        public void fireWrite(AMQPHeader header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write AMQP Header event."));
            }
        }

        @Override
        public void fireWrite(Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write AMQP performative event."));
            }
        }

        @Override
        public void fireWrite(SaslPerformative performative) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write SASL performative event."));
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

    //----- Default TransportHandler Used at the pipeline boundry ------------//

    private class BoundryTransportHandler implements TransportHandler {

        @Override
        public void handlerAdded(TransportHandlerContext context) throws Exception {
        }

        @Override
        public void handlerRemoved(TransportHandlerContext context) throws Exception {
        }

        @Override
        public void handleRead(TransportHandlerContext context, ProtonBuffer buffer) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed Transport read event."));
            }
        }

        @Override
        public void handleRead(TransportHandlerContext context, HeaderFrame header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed AMQP Header event."));
            }
        }

        @Override
        public void handleRead(TransportHandlerContext context, SaslFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed SASL frame event."));
            }
        }

        @Override
        public void handleRead(TransportHandlerContext context, ProtocolFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed protocol frame event."));
            }
        }

        @Override
        public void transportEncodingError(TransportHandlerContext context, Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed encoding error.", e));
            }
        }

        @Override
        public void transportDecodingError(TransportHandlerContext context, Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed decoding error.", e));
            }
        }

        @Override
        public void transportFailed(TransportHandlerContext context, Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport, e);
            }
        }

        @Override
        public void handleWrite(TransportHandlerContext context, AMQPHeader header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write AMQP Header event."));
            }
        }

        @Override
        public void handleWrite(TransportHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write AMQP performative event."));
            }
        }

        @Override
        public void handleWrite(TransportHandlerContext context, SaslPerformative performative) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write SASL performative event."));
            }
        }

        @Override
        public void handleWrite(TransportHandlerContext context, Frame<ProtonBuffer> frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write Frame event."));
            }
        }

        @Override
        public void handleWrite(TransportHandlerContext context, ProtonBuffer buffer) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed write data event."));
            }
        }

        @Override
        public void handleFlush(TransportHandlerContext context) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            TransportListener listener = transport.getTransportListener();
            if (listener != null) {
                listener.onTransportFailed(transport,
                    new IOException("No handler processed flush event."));
            }
        }
    }
}
