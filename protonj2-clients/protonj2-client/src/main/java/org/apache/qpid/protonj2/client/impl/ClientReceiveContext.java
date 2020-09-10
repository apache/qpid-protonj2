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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.InputStreamOptions;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.ReceiveContext;
import org.apache.qpid.protonj2.client.ReceiveContextOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryAbortedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryIsPartialException;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Receive Context used to request reads of possible split framed
 * {@link Transfer} payload's that comprise a single large {@link Delivery}.
 */
public class ClientReceiveContext implements ReceiveContext {

    private final ClientReceiver receiver;
    @SuppressWarnings("unused")
    private final ReceiveContextOptions options;

    private ClientDelivery delivery;

    ClientReceiveContext(ClientReceiver receiver, ReceiveContextOptions options) {
        this.receiver = receiver;
        this.options = new ReceiveContextOptions(options);
    }

    @Override
    public Delivery delivery() {
        return delivery;
    }

    @Override
    public ClientReceiver receiver() {
        return receiver;
    }

    @Override
    public <E> Message<E> message() throws ClientException {
        if (delivery != null) {
            if (complete()) {
                return delivery.message();
            } else if (aborted()) {
                throw new ClientDeliveryAbortedException("Cannot read Message contents from an aborted delivery.");
            } else {
                throw new ClientDeliveryIsPartialException("Cannot read Message contents from a partially received delivery.");
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean aborted() {
        if (delivery != null && delivery.protonDelivery() != null) {
            return delivery.protonDelivery().isAborted();
        } else {
            return false;
        }
    }

    @Override
    public boolean complete() {
        if (delivery != null && delivery.protonDelivery() != null) {
            return !delivery.protonDelivery().isPartial() && !delivery.protonDelivery().isAborted();
        } else {
            return false;
        }
    }

    @Override
    public InputStream rawInputStream(InputStreamOptions options) throws ClientException {
        checkClosed();
        checkAborted();

        return null;
    }

    private void checkClosed() throws ClientIllegalStateException {
        if (receiver.isClosed()) {
            throw new ClientIllegalStateException("The parent Receiver instance has already been closed.");
        }
    }

    private void checkAborted() throws ClientIllegalStateException {
        if (aborted()) {
            throw new ClientIllegalStateException("The incoming delivery was aborted.");
        }
    }

    //----- Internal InputStream implementations

    @SuppressWarnings("unused")
    private class RawInputStream extends InputStream {

        protected final AtomicBoolean closed = new AtomicBoolean();
        protected final InputStreamOptions options;

        public RawInputStream(InputStreamOptions options) {
            this.options = options;
        }

        @Override
        public int read() throws IOException {
            throw new IOException();
        }

        private void checkClosed() throws IOException {
            if (closed.get()) {
                throw new IOException("The InputStream has already been closed.");
            }

            if (receiver.isClosed()) {
                throw new IOException("The parent Receiver instance has already been closed.");
            }
        }
    }
}
