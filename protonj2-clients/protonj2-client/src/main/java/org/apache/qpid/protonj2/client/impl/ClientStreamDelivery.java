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
import java.util.concurrent.ScheduledExecutorService;

import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.exceptions.ClientDeliveryAbortedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientResourceRemotelyClosedException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.engine.util.StringUtils;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Rejected;
import org.apache.qpid.protonj2.types.messaging.Released;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;

public class ClientStreamDelivery implements StreamDelivery {

    private final ClientStreamReceiver receiver;
    private final IncomingDelivery protonDelivery;

    private ClientStreamReceiverMessage message;
    private RawDeliveryInputStream rawInputStream;

    public ClientStreamDelivery(ClientStreamReceiver receiver, IncomingDelivery protonDelivery) {
        this.receiver = receiver;
        this.protonDelivery = protonDelivery.setLinkedResource(this);

        // Capture inbound events and route to an active stream or message
        protonDelivery.deliveryReadHandler(this::handleDeliveryRead)
                      .deliveryAbortedHandler(this::handleDeliveryAborted);
    }

    IncomingDelivery getProtonDelivery() {
        return protonDelivery;
    }

    @Override
    public ClientStreamReceiver receiver() {
        return receiver;
    }

    @Override
    public boolean aborted() {
        return protonDelivery.isAborted();
    }

    @Override
    public boolean completed() {
        return !protonDelivery.isPartial();
    }

    @Override
    public int messageFormat() {
        return protonDelivery.getMessageFormat();
    }

    @Override
    public ClientStreamReceiverMessage message() throws ClientException {
        if (rawInputStream != null) {
            throw new ClientIllegalStateException("Cannot access Delivery Message API after requesting an InputStream");
        }

        if (message == null) {
            message = new ClientStreamReceiverMessage(receiver, this);
        }

        return message;
    }

    @Override
    public Map<String, Object> annotations() throws ClientException {
        if (rawInputStream != null) {
            throw new ClientIllegalStateException("Cannot access Delivery Annotations API after requesting an InputStream");
        }

        if (message == null) {
            message = new ClientStreamReceiverMessage(receiver, this);
        }

        return StringUtils.toStringKeyedMap(message.deliveryAnnotations() != null ? message.deliveryAnnotations().getValue() : null);
    }

    @Override
    public InputStream rawInputStream() throws ClientException {
        if (message != null) {
            throw new ClientIllegalStateException("Cannot access Delivery InputStream API after requesting an Message");
        }

        if (rawInputStream == null) {
            rawInputStream = new RawDeliveryInputStream();
        }

        return rawInputStream;
    }

    @Override
    public StreamDelivery accept() throws ClientException {
        receiver.disposition(protonDelivery, Accepted.getInstance(), true);
        return this;
    }

    @Override
    public StreamDelivery release() throws ClientException {
        receiver.disposition(protonDelivery, Released.getInstance(), true);
        return this;
    }

    @Override
    public StreamDelivery reject(String condition, String description) throws ClientException {
        receiver.disposition(protonDelivery, new Rejected().setError(new ErrorCondition(condition, description)), true);
        return this;
    }

    @Override
    public StreamDelivery modified(boolean deliveryFailed, boolean undeliverableHere) throws ClientException {
        receiver.disposition(protonDelivery, new Modified().setDeliveryFailed(deliveryFailed).setUndeliverableHere(undeliverableHere), true);
        return this;
    }

    @Override
    public StreamDelivery disposition(DeliveryState state, boolean settle) throws ClientException {
        receiver.disposition(protonDelivery, ClientDeliveryState.asProtonType(state), settle);
        return this;
    }

    @Override
    public StreamDelivery settle() throws ClientException {
        receiver.disposition(protonDelivery, null, true);
        return this;
    }

    @Override
    public DeliveryState state() {
        return ClientDeliveryState.fromProtonType(protonDelivery.getState());
    }

    @Override
    public boolean settled() {
        return protonDelivery.isSettled();
    }

    @Override
    public DeliveryState remoteState() {
        return ClientDeliveryState.fromProtonType(protonDelivery.getRemoteState());
    }

    @Override
    public boolean remoteSettled() {
        return protonDelivery.isRemotelySettled();
    }

    //----- Event Handlers for Delivery updates

    void handleDeliveryRead(IncomingDelivery delivery) {
        if (message != null) {
            message.handleDeliveryRead(delivery);
        }
        if (rawInputStream != null) {
            rawInputStream.handleDeliveryRead(delivery);
        }
    }

    void handleDeliveryAborted(IncomingDelivery delivery) {
        if (message != null) {
            message.handleDeliveryAborted(delivery);
        }
        if (rawInputStream != null) {
            rawInputStream.handleDeliveryAborted(delivery);
        }
    }

    void handleReceiverClosed(ClientStreamReceiver receiver) {
        if (message != null) {
            message.handleReceiverClosed(receiver);
        }
        if (rawInputStream != null) {
            rawInputStream.handleReceiverClosed(receiver);
        }
    }

    //----- Raw InputStream Implementation

    private class RawDeliveryInputStream extends InputStream {

        private final ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        private final ScheduledExecutorService executor = receiver.session().getScheduler();

        private ClientFuture<Integer> readRequest;

        @Override
        public boolean markSupported() {
            return false; // Read bytes can be discarded to marks cannot be supported.
        }

        @Override
        public int available() throws IOException {
            checkStreamStateIsValid();

            // Check for any bytes in the delivery that haven't been moved to the read buffer yet
            if (buffer.isReadable()) {
                return buffer.getReadableBytes();
            } else {
                final ClientFuture<Integer> request = receiver.session().getFutureFactory().createFuture();

                try {
                    executor.execute(() -> {
                        if (protonDelivery.available() > 0) {
                            buffer.append(protonDelivery.readAll());
                        }

                        request.complete(buffer.getReadableBytes());
                    });

                    return receiver.session().request(receiver, request);
                } catch (Exception e) {
                    throw new IOException("Error reading requested data", e);
                }
            }
        }

        @Override
        public int read() throws IOException {
            checkStreamStateIsValid();

            int result = -1;

            while (true) {
                if (buffer.isReadable()) {
                    result = buffer.readByte();
                    // TODO: Reclaim read buffers for GC
                    // buffer.releaseReadBuffers
                    break;
                } else {
                    final ClientFuture<Integer> request = receiver.session().getFutureFactory().createFuture();

                    try {
                        executor.execute(() -> {
                            if (protonDelivery.available() > 0) {
                                buffer.append(protonDelivery.readAll());
                                request.complete(buffer.getReadableBytes());
                            } else if (!protonDelivery.isPartial() || protonDelivery.isAborted()) {
                                request.complete(-1);
                            } else {
                                readRequest = request;
                            }
                        });

                        if (receiver.session().request(receiver, request) == -1) {
                            break;
                        }
                    } catch (Exception e) {
                        throw new IOException("Error reading requested data", e);
                    }
                }
            }

            return result;
        }

        void handleDeliveryRead(IncomingDelivery delivery) {
            // An input stream is awaiting some more incoming bytes, check to see if
            // the delivery had a non-empty transfer frame and provide them.
            if (readRequest != null && delivery.available() > 0) {
                buffer.append(protonDelivery.readAll());
                readRequest.complete(buffer.getReadableBytes());
            }
        }

        void handleDeliveryAborted(IncomingDelivery delivery) {
            if (readRequest != null) {
                readRequest.failed(new ClientDeliveryAbortedException("The remote sender has aborted this delivery"));
            }
        }

        void handleReceiverClosed(ClientStreamReceiver receiver) {
            if (readRequest != null) {
                readRequest.failed(new ClientResourceRemotelyClosedException("The receliver link has been remotely closed."));
            }
        }

        private void checkStreamStateIsValid() throws IOException {
            if (receiver.isClosed()) {
                throw new IOException("Underlying receiver has closed", receiver.getFailureCause());
            }
        }
    }
}
