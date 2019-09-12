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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.engine.EngineHandler;
import org.apache.qpid.proton4j.engine.EngineHandlerContext;
import org.apache.qpid.proton4j.engine.EnginePipeline;
import org.apache.qpid.proton4j.engine.EventHandler;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.SaslFrame;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;
import org.apache.qpid.proton4j.engine.exceptions.ProtonExceptionSupport;

/**
 * Pipeline of TransportHandlers used to process IO
 */
public class ProtonEnginePipeline implements EnginePipeline {

    TransportHandlerContextReadBoundry head;
    TransportHandlerContextWriteBoundry tail;

    private final ProtonEngine engine;

    ProtonEnginePipeline(ProtonEngine engine) {
        if (engine == null) {
            throw new IllegalArgumentException("Parent transport cannot be null");
        }

        this.engine = engine;

        head = new TransportHandlerContextReadBoundry();
        tail = new TransportHandlerContextWriteBoundry();

        // Ensure Pipeline starts out empty but initialized.
        head.next = tail;
        head.previous = head;

        tail.previous = head;
        tail.next = tail;
    }

    @Override
    public ProtonEngine engine() {
        return engine;
    }

    @Override
    public ProtonEnginePipeline addFirst(String name, EngineHandler handler) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Handler name cannot be null or empty");
        }

        if (handler == null) {
            throw new IllegalArgumentException("Handler provided cannot be null");
        }

        ProtonEngineHandlerContext oldFirst = head.next;
        ProtonEngineHandlerContext newFirst = createContext(name, handler);

        newFirst.next = oldFirst;
        newFirst.previous = head;

        oldFirst.previous = newFirst;
        head.next = newFirst;

        try {
            newFirst.getHandler().handlerAdded(newFirst);
        } catch (Exception e) {
            // TODO - Log the error and move on ?
        }

        return this;
    }

    @Override
    public ProtonEnginePipeline addLast(String name, EngineHandler handler) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Handler name cannot be null or empty");
        }

        if (handler == null) {
            throw new IllegalArgumentException("Handler provided cannot be null");
        }

        ProtonEngineHandlerContext oldLast = tail.previous;
        ProtonEngineHandlerContext newLast = createContext(name, handler);

        newLast.next = tail;
        newLast.previous = oldLast;

        oldLast.next = newLast;
        tail.previous = newLast;

        try {
            newLast.getHandler().handlerAdded(newLast);
        } catch (Exception e) {
            // TODO - Log the error and move on ?
        }

        return this;
    }

    @Override
    public ProtonEnginePipeline removeFirst() {
        if (head.next != tail) {
            ProtonEngineHandlerContext oldFirst = head.next;

            head.next = oldFirst.next;
            head.next.previous = head;

            try {
                oldFirst.getHandler().handlerRemoved(oldFirst);
            } catch (Exception e) {
                // TODO - Log the error and move on ?
            }
        }

        return this;
    }

    @Override
    public ProtonEnginePipeline removeLast() {
        if (tail.previous != head) {
            ProtonEngineHandlerContext oldLast = tail.previous;

            tail.previous = oldLast.previous;
            tail.previous.next = tail;

            try {
                oldLast.getHandler().handlerRemoved(oldLast);
            } catch (Exception e) {
                // TODO - Log the error and move on ?
            }
        }

        return this;
    }

    @Override
    public ProtonEnginePipeline remove(String name) {
        if (name != null && !name.isEmpty()) {
            ProtonEngineHandlerContext current = head.next;
            ProtonEngineHandlerContext removed = null;
            while (current != tail) {
                if (current.getName().equals(name)) {
                    removed = current;

                    ProtonEngineHandlerContext newNext = current.next;

                    current.previous.next = newNext;
                    newNext.previous = current.previous;

                    break;
                }

                current = current.next;
            }

            if (removed != null) {
                try {
                    removed.getHandler().handlerRemoved(removed);
                } catch (Exception e) {
                    // TODO - Log the error and move on ?
                }
            }
        }

        return this;
    }

    @Override
    public EngineHandler first() {
        return head.next == tail ? null : head.next.getHandler();
    }

    @Override
    public EngineHandler last() {
        return tail.previous == head ? null : tail.previous.getHandler();
    }

    @Override
    public EngineHandlerContext firstContext() {
        return head.next == tail ? null : head.next;
    }

    @Override
    public EngineHandlerContext lastContext() {
        return tail.previous == head ? null : tail.previous;
    }

    //----- Event injection methods ------------------------------------------//

    @Override
    public ProtonEnginePipeline fireEngineStarting() {
        head.fireEngineStarting();
        return this;
    }

    @Override
    public ProtonEnginePipeline fireEngineStateChanged() {
        head.fireEngineStateChanged();
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(ProtonBuffer input) {
        tail.fireRead(input);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(HeaderFrame header) {
        tail.fireRead(header);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(SaslFrame frame) {
        tail.fireRead(frame);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(ProtocolFrame frame) {
        tail.fireRead(frame);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(AMQPHeader header) {
        head.fireWrite(header);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        head.fireWrite(performative, channel, payload, payloadToLarge);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(SaslPerformative performative) {
        head.fireWrite(performative);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(ProtonBuffer buffer) {
        head.fireWrite(buffer);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireEncodingError(Throwable e) {
        tail.fireEncodingError(e);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireDecodingError(Throwable e) {
        tail.fireDecodingError(e);
        return this;
    }

    @Override
    public ProtonEnginePipeline fireFailed(Throwable e) {
        tail.fireFailed(e);
        return this;
    }

    //----- Internal implementation ------------------------------------------//

    private ProtonEngineHandlerContext createContext(String name, EngineHandler handler) {
        return new ProtonEngineHandlerContext(name, engine, handler);
    }

    //----- Synthetic handler context that bounds the pipeline ---------------//

    private class TransportHandlerContextReadBoundry extends ProtonEngineHandlerContext {

        public TransportHandlerContextReadBoundry() {
            super("Read Boundry", engine, new BoundryTransportHandler());
        }

        @Override
        public void fireRead(ProtonBuffer buffer) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed Transport read event."));
            }
        }

        @Override
        public void fireRead(HeaderFrame header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed AMQP Header event."));
            }
        }

        @Override
        public void fireRead(SaslFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed SASL frame event."));
            }
        }

        @Override
        public void fireRead(ProtocolFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed protocol frame event."));
            }
        }

        @Override
        public void fireEncodingError(Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed encoding error.", e));
            }
        }

        @Override
        public void fireDecodingError(Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed decoding error.", e));
            }
        }

        @Override
        public void fireFailed(Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(ProtonExceptionSupport.create(e));
            }
        }
    }

    private class TransportHandlerContextWriteBoundry extends ProtonEngineHandlerContext {

        public TransportHandlerContextWriteBoundry() {
            super("Write Boundry", engine, new BoundryTransportHandler());
        }

        @Override
        public void fireWrite(AMQPHeader header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed write AMQP Header event."));
            }
        }

        @Override
        public void fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed write AMQP performative event."));
            }
        }

        @Override
        public void fireWrite(SaslPerformative performative) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed write SASL performative event."));
            }
        }

        @Override
        public void fireWrite(ProtonBuffer buffer) {
            // When not handled in the handler chain the buffer write propagates to the
            // engine to be handed to any registered output handler.  The engine is then
            // responsible for error handling if nothing is registered there to handle the
            // output of frame data.
            engine.dispatchWriteToEventHandler(buffer);
        }
    }

    //----- Default TransportHandler Used at the pipeline boundry ------------//

    private class BoundryTransportHandler implements EngineHandler {

        @Override
        public void engineStarting(EngineHandlerContext context) {
        }

        @Override
        public void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed Transport read event."));
            }
        }

        @Override
        public void handleRead(EngineHandlerContext context, HeaderFrame header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed AMQP Header event."));
            }
        }

        @Override
        public void handleRead(EngineHandlerContext context, SaslFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed SASL frame event."));
            }
        }

        @Override
        public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed protocol frame event."));
            }
        }

        @Override
        public void transportEncodingError(EngineHandlerContext context, Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed encoding error.", e));
            }
        }

        @Override
        public void transportDecodingError(EngineHandlerContext context, Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed decoding error.", e));
            }
        }

        @Override
        public void transportFailed(EngineHandlerContext context, Throwable e) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(ProtonExceptionSupport.create(e));
            }
        }

        @Override
        public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed write AMQP Header event."));
            }
        }

        @Override
        public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed write AMQP performative event."));
            }
        }

        @Override
        public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
            // TODO Decide on the exact error to be fired, move Transport to failed state.
            EventHandler<Throwable> handler = engine.errorHandler();
            if (handler != null) {
                handler.handle(new ProtonException("No handler processed write SASL performative event."));
            }
        }
    }
}
