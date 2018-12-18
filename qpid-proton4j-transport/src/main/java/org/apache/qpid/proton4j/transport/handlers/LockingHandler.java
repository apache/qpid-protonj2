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

import java.util.concurrent.locks.ReentrantLock;

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

/**
 * Handler that prevents concurrent reads and writes
 *
 * TODO - Possible this can be more trouble than it is worth compared to just making it
 *        known that read / write must be single threaded ?
 */
public class LockingHandler implements TransportHandler {

    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public void handleRead(TransportHandlerContext context, ProtonBuffer buffer) {
        lock.lock();
        try {
            context.fireRead(buffer);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleRead(TransportHandlerContext context, HeaderFrame header) {
        lock.lock();
        try {
            context.fireRead(header);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleRead(TransportHandlerContext context, SaslFrame frame) {
        lock.lock();
        try {
            context.fireRead(frame);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleRead(TransportHandlerContext context, ProtocolFrame frame) {
        lock.lock();
        try {
            context.fireRead(frame);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void transportEncodingError(TransportHandlerContext context, Throwable e) {
        lock.lock();
        try {
            context.fireEncodingError(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void transportDecodingError(TransportHandlerContext context, Throwable e) {
        lock.lock();
        try {
            context.fireDecodingError(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void transportFailed(TransportHandlerContext context, Throwable e) {
        lock.lock();
        try {
            context.fireFailed(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleWrite(TransportHandlerContext context, AMQPHeader header) {
        lock.lock();
        try {
            context.fireWrite(header);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleWrite(TransportHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge) {
        lock.lock();
        try {
            context.fireWrite(performative, channel, payload, payloadToLarge);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleWrite(TransportHandlerContext context, SaslPerformative performative) {
        lock.lock();
        try {
            context.fireWrite(performative);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleWrite(TransportHandlerContext context, Frame<ProtonBuffer> frame) {
        lock.lock();
        try {
            context.fireWrite(frame);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleWrite(TransportHandlerContext context, ProtonBuffer buffer) {
        lock.lock();
        try {
            context.fireWrite(buffer);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleFlush(TransportHandlerContext context) {
        lock.lock();
        try {
            context.fireFlush();
        } finally {
            lock.unlock();
        }
    }
}
