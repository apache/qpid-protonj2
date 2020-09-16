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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.ReceiveContext;
import org.apache.qpid.protonj2.client.ReceiveContextOptions;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Receive Context used to request reads of possible split framed
 * {@link Transfer} payload's that comprise a single large {@link Delivery}.
 */
public class ClientReceiveContext implements ReceiveContext {

    private final ClientReceiver receiver;
    @SuppressWarnings("unused")
    private final ReceiveContextOptions options;
    private final ClientFuture<ClientDelivery> deliveryFuture;

    private ClientDelivery delivery;

    ClientReceiveContext(ClientReceiver receiver, ReceiveContextOptions options) {
        this.receiver = receiver;
        this.options = new ReceiveContextOptions(options);

        this.deliveryFuture = receiver.session().getFutureFactory().createFuture();
    }

    @Override
    public ClientReceiveContext awaitDelivery() throws ClientException {
        if (!deliveryFuture.isComplete()) {
            try {
                receiver.attachToNextDelivery(this);
                delivery = deliveryFuture.get();
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ClientException("Wait for delivery was interrupted", e);
            } catch (ExecutionException e) {
                throw ClientExceptionSupport.createNonFatalOrPassthrough(e.getCause());
            }
        }

        return this;
    }

    @Override
    public ClientReceiveContext awaitDelivery(long timeout, TimeUnit unit) throws ClientException {
        if (!deliveryFuture.isComplete()) {
            try {
                receiver.attachToNextDelivery(this);

                // TODO: What happens after the timeout, the context is still attached.
                delivery = deliveryFuture.get(timeout, unit);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ClientException("Wait for delivery was interrupted", e);
            } catch (ExecutionException e) {
                throw ClientExceptionSupport.createNonFatalOrPassthrough(e.getCause());
            } catch (TimeoutException e) {
                throw new ClientOperationTimedOutException("Timed out waiting for new Delivery", e);
            }
        }

        return this;
    }

    @Override
    public Delivery delivery() {
        return delivery;
    }

    @Override
    public Receiver receiver() {
        return receiver;
    }

    @Override
    public <E> Message<E> message() throws ClientException {
        if (delivery != null) {
            return delivery.message();
        } else {
            throw new ClientIllegalStateException("Cannot read a message until the context has a complete delivery");
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
    public boolean completed() {
        if (delivery != null && delivery.protonDelivery() != null) {
            return !delivery.protonDelivery().isPartial() && !delivery.protonDelivery().isAborted();
        } else {
            return false;
        }
    }

    @Override
    public InputStream rawInputStream() throws ClientException {
        checkClosed();
        checkAborted();

        return new RawInputStream();
    }

    /*
     * Called within the event loop thread from the parent receiver.
     */
    void handleDeliveryRead(ClientDelivery delivery) {
        // Is this the first delivery of is this a new delivery in an ongoing
        // transfer of a larger message set.
        if (!deliveryFuture.isComplete()) {
            deliveryFuture.complete(delivery.abortedHandler(this::handleDeliveryAborted));
        } else {
            // Ongoing processing kicks in now and fills read buffer
        }
    }

    /*
     * Called within the event loop thread from the parent receiver.
     */
    void handleDeliveryAborted(ClientDelivery delivery) {
        // Need to abort blocked reads waiting for new data.
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
