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

import org.apache.qpid.proton4j.transport.TransportHandler;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;
import org.apache.qpid.proton4j.transport.TransportPipeline;

/**
 * Pipeline of TransportHandlers used to process IO
 */
public class ProtonTransportPipeline implements TransportPipeline {

    TransportHandlerContext head;
    TransportHandlerContext tail;

    private final ProtonTransport transport;

    ProtonTransportPipeline(ProtonTransport transport) {
        if (transport == null) {
            throw new IllegalArgumentException("Parent transport cannot be null");
        }

        this.transport = transport;
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
}
