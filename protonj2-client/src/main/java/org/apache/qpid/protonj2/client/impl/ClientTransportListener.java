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
package org.apache.qpid.protonj2.client.impl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.transport.Transport;
import org.apache.qpid.protonj2.client.transport.TransportListener;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.exceptions.EngineStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transport events listener that is bound to a single proton {@link Engine} instance
 * for its lifetime which prevent duplication of error or connection closed events from
 * influencing a {@link ClientConnection} that will attempt reconnection.
 */
final class ClientTransportListener implements TransportListener {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTransportListener.class);

    private final Engine engine;

    ClientTransportListener(Engine engine) {
        this.engine = engine;
    }

    @Override
    public void transportInitialized(Transport transport) {
        engine.configuration().setBufferAllocator(transport.getBufferAllocator());
    }

    @Override
    public void transportConnected(Transport transport) {
        engine.start().open();
    }

    @Override
    public void transportRead(ProtonBuffer incoming) {
        try {
            do {
                engine.ingest(incoming);
            } while (incoming.isReadable() && engine.isWritable());
            // TODO - How do we handle case of not all data read ?
        } catch (EngineStateException e) {
            LOG.warn("Caught problem during incoming data processing: {}", e.getMessage(), e);
            engine.engineFailed(ClientExceptionSupport.createOrPassthroughFatal(e));
        }
    }

    @Override
    public void transportError(Throwable error) {
        if (!engine.isShutdown()) {
            LOG.debug("Transport failed: {}", error.getMessage());
            engine.engineFailed(ClientExceptionSupport.convertToConnectionClosedException(error));
        }
    }
}
