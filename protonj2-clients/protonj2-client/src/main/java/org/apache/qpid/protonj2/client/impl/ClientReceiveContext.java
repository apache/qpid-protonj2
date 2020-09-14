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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.InputStreamOptions;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.ReceiveContext;
import org.apache.qpid.protonj2.client.ReceiveContextOptions;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryAbortedException;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryIsPartialException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Receive Context used to request reads of possible split framed
 * {@link Transfer} payload's that comprise a single large {@link Delivery}.
 */
public class ClientReceiveContext implements ReceiveContext {

    private final ClientReceiver receiver;
    @SuppressWarnings("unused")
    private final ReceiveContextOptions options;
    private final ClientFuture<ClientReceiveContextDelivery> deliveryFuture;

    private ClientReceiveContextDelivery delivery;

    ClientReceiveContext(ClientReceiver receiver, ReceiveContextOptions options) {
        this.receiver = receiver;
        this.options = new ReceiveContextOptions(options);

        this.deliveryFuture = receiver.session().getFutureFactory().createFuture();
    }

    @Override
    public ClientReceiveContext awaitDelivery() throws ClientException {
        if (!deliveryFuture.isComplete()) {
            try {
                this.delivery = deliveryFuture.get();
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
                this.delivery = deliveryFuture.get(timeout, unit);
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

    void handleDeliveryRead(ClientDelivery delivery) {
        delivery.protonDelivery().setLinkedResource(this);

        // Is this the first delivery of is this a new delivery in an ongoing
        // transfer of a larger message set.
        if (deliveryFuture.isComplete()) {
            deliveryFuture.complete(new ClientReceiveContextDelivery(delivery));
        } else {

        }
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

    /*
     * The context requires its own delivery implementation to allow for handling calls to
     * the delivery state modifiers like release or reject as the stream would need to be
     * unblocked by those if a read was blocked.
     */
    private class ClientReceiveContextDelivery implements Delivery {

        private final ClientDelivery delivery;

        public ClientReceiveContextDelivery(ClientDelivery delivery) {
            this.delivery = delivery;
        }

        public IncomingDelivery protonDelivery() {
            return delivery.protonDelivery();
        }

        @Override
        public Receiver receiver() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public <E> Message<E> message() throws ClientException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Map<String, Object> deliveryAnnotations() throws ClientException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Delivery accept() throws ClientException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Delivery release() throws ClientException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Delivery reject(String condition, String description) throws ClientException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Delivery modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Delivery disposition(DeliveryState state, boolean settle) throws ClientException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Delivery settle() throws ClientException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean settled() {
            return delivery.settled();
        }

        @Override
        public DeliveryState state() {
            return delivery.state();
        }

        @Override
        public DeliveryState remoteState() {
            return delivery.remoteState();
        }

        @Override
        public boolean remoteSettled() {
            return delivery.remoteSettled();
        }

        @Override
        public int messageFormat() {
            return delivery.messageFormat();
        }
    }

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
