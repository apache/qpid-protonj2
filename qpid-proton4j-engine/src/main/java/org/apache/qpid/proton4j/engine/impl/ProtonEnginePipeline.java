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
import org.apache.qpid.proton4j.engine.EngineState;
import org.apache.qpid.proton4j.engine.HeaderFrame;
import org.apache.qpid.proton4j.engine.ProtocolFrame;
import org.apache.qpid.proton4j.engine.SaslFrame;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;

/**
 * Pipeline of TransportHandlers used to process IO
 */
public class ProtonEnginePipeline implements EnginePipeline {

    EngineHandlerContextReadBoundry head;
    EngineHandlerContextWriteBoundry tail;

    private final ProtonEngine engine;

    ProtonEnginePipeline(ProtonEngine engine) {
        if (engine == null) {
            throw new IllegalArgumentException("Parent transport cannot be null");
        }

        this.engine = engine;

        head = new EngineHandlerContextReadBoundry();
        tail = new EngineHandlerContextWriteBoundry();

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
        } catch (Throwable e) {
            engine.engineFailed(e);
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
        } catch (Throwable e) {
            engine.engineFailed(e);
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
            } catch (Throwable e) {
                engine.engineFailed(e);
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
            } catch (Throwable e) {
                engine.engineFailed(e);
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
                } catch (Throwable e) {
                    engine.engineFailed(e);
                }
            }
        }

        return this;
    }

    @Override
    public EnginePipeline remove(EngineHandler handler) {
        if (handler != null) {
            ProtonEngineHandlerContext current = head.next;
            ProtonEngineHandlerContext removed = null;
            while (current != tail) {
                if (current.getHandler() == handler) {
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
                } catch (Throwable e) {
                    engine.engineFailed(e);
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

    //----- Event injection methods

    @Override
    public ProtonEnginePipeline fireEngineStarting() {
        ProtonEngineHandlerContext current = head;
        while (current != tail) {
            if (engine.state().ordinal() < EngineState.SHUTTING_DOWN.ordinal()) {
                try {
                    current.fireEngineStarting();
                } catch (Throwable error) {
                    engine.engineFailed(error);
                    break;
                }
                current = current.next;
            }
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireEngineStateChanged() {
        try {
            head.fireEngineStateChanged();
        } catch (Throwable error) {
            engine.engineFailed(error);
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(ProtonBuffer input) {
        try {
            tail.fireRead(input);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(HeaderFrame header) {
        try {
            tail.fireRead(header);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(SaslFrame frame) {
        try {
            tail.fireRead(frame);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireRead(ProtocolFrame frame) {
        try {
            tail.fireRead(frame);
        } catch (Throwable error) {
            engine.engineFailed(error);
            throw error;
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(AMQPHeader header) {
        try {
            head.fireWrite(header);
        } catch (Throwable error) {
            engine.engineFailed(error);
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
        try {
            head.fireWrite(performative, channel, payload, payloadToLarge);
        } catch (Throwable error) {
            engine.engineFailed(error);
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(SaslPerformative performative) {
        try {
            head.fireWrite(performative);
        } catch (Throwable error) {
            engine.engineFailed(error);
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireWrite(ProtonBuffer buffer) {
        try {
            head.fireWrite(buffer);
        } catch (Throwable error) {
            engine.engineFailed(error);
        }
        return this;
    }

    @Override
    public ProtonEnginePipeline fireFailed(Throwable e) {
        try {
            tail.fireFailed(e);
        } catch (Throwable error) {
            engine.engineFailed(error);
        }
        return this;
    }

    //----- Internal implementation

    private ProtonEngineHandlerContext createContext(String name, EngineHandler handler) {
        return new ProtonEngineHandlerContext(name, engine, handler);
    }

    //----- Synthetic handler context that bounds the pipeline

    private class EngineHandlerContextReadBoundry extends ProtonEngineHandlerContext {

        public EngineHandlerContextReadBoundry() {
            super("Read Boundry", engine, new BoundryEngineHandler());
        }

        @Override
        public void fireRead(ProtonBuffer buffer) {
            engine.engineFailed(new ProtonException("No handler processed Transport read event."));
        }

        @Override
        public void fireRead(HeaderFrame header) {
            engine.engineFailed(new ProtonException("No handler processed AMQP Header event."));
        }

        @Override
        public void fireRead(SaslFrame frame) {
            engine.engineFailed(new ProtonException("No handler processed SASL frame event."));
        }

        @Override
        public void fireRead(ProtocolFrame frame) {
            engine.engineFailed(new ProtonException("No handler processed protocol frame event."));
        }

        @Override
        public void fireFailed(Throwable e) {
            engine.engineFailed(e);
        }
    }

    private class EngineHandlerContextWriteBoundry extends ProtonEngineHandlerContext {

        public EngineHandlerContextWriteBoundry() {
            super("Write Boundry", engine, new BoundryEngineHandler());
        }

        @Override
        public void fireWrite(AMQPHeader header) {
            engine.engineFailed(new ProtonException("No handler processed write AMQP Header event."));
        }

        @Override
        public void fireWrite(Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
            engine.engineFailed(new ProtonException("No handler processed write AMQP performative event."));
        }

        @Override
        public void fireWrite(SaslPerformative performative) {
            engine.engineFailed(new ProtonException("No handler processed write SASL performative event."));
        }

        @Override
        public void fireWrite(ProtonBuffer buffer) {
            // When not handled in the handler chain the buffer write propagates to the
            // engine to be handed to any registered output handler.  The engine is then
            // responsible for error handling if nothing is registered there to handle the
            // output of frame data.
            try {
                engine.dispatchWriteToEventHandler(buffer);
            } catch (Throwable error) {
                engine.engineFailed(error);
            }
        }
    }

    //----- Default TransportHandler Used at the pipeline boundary

    private class BoundryEngineHandler implements EngineHandler {

        @Override
        public void engineStarting(EngineHandlerContext context) {
        }

        @Override
        public void handleRead(EngineHandlerContext context, ProtonBuffer buffer) {
            engine.engineFailed(new ProtonException("No handler processed Transport read event."));
        }

        @Override
        public void handleRead(EngineHandlerContext context, HeaderFrame header) {
            engine.engineFailed(new ProtonException("No handler processed AMQP Header event."));
        }

        @Override
        public void handleRead(EngineHandlerContext context, SaslFrame frame) {
            engine.engineFailed(new ProtonException("No handler processed SASL frame event."));
        }

        @Override
        public void handleRead(EngineHandlerContext context, ProtocolFrame frame) {
            engine.engineFailed(new ProtonException("No handler processed protocol frame event."));
        }

        @Override
        public void engineFailed(EngineHandlerContext context, Throwable e) {
            engine.engineFailed(e);
        }

        @Override
        public void handleWrite(EngineHandlerContext context, AMQPHeader header) {
            engine.engineFailed(new ProtonException("No handler processed write AMQP Header event."));
        }

        @Override
        public void handleWrite(EngineHandlerContext context, Performative performative, int channel, ProtonBuffer payload, Runnable payloadToLarge) {
            engine.engineFailed(new ProtonException("No handler processed write AMQP performative event."));
        }

        @Override
        public void handleWrite(EngineHandlerContext context, SaslPerformative performative) {
            engine.engineFailed(new ProtonException("No handler processed write SASL performative event."));
        }
    }
}
