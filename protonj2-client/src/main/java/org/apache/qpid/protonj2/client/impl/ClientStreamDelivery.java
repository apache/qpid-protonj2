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
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ClientStreamDelivery implements StreamDelivery {

    private static final Logger LOG = LoggerFactory.getLogger(ClientStreamDelivery.class);

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
        if (rawInputStream != null && message == null) {
            throw new ClientIllegalStateException("Cannot access Delivery Message API after requesting an InputStream");
        }

        if (message == null) {
            message = new ClientStreamReceiverMessage(receiver, this, rawInputStream = new RawDeliveryInputStream());
        }

        return message;
    }

    @Override
    public Map<String, Object> annotations() throws ClientException {
        if (rawInputStream != null && message == null) {
            throw new ClientIllegalStateException("Cannot access Delivery Annotations API after requesting an InputStream");
        }

        return StringUtils.toStringKeyedMap(message().deliveryAnnotations() != null ? message().deliveryAnnotations().getValue() : null);
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
        if (rawInputStream != null) {
            rawInputStream.handleDeliveryRead(delivery);
        }
    }

    void handleDeliveryAborted(IncomingDelivery delivery) {
        if (rawInputStream != null) {
            rawInputStream.handleDeliveryAborted(delivery);
        }
    }

    void handleReceiverClosed(ClientStreamReceiver receiver) {
        if (rawInputStream != null) {
            rawInputStream.handleReceiverClosed(receiver);
        }
    }

    //----- Raw InputStream Implementation

    private class RawDeliveryInputStream extends InputStream {

        private final int INVALID_MARK = -1;

        private final ProtonCompositeBuffer buffer = new ProtonCompositeBuffer();
        private final ScheduledExecutorService executor = receiver.session().getScheduler();

        private ClientFuture<Integer> readRequest;

        private AtomicBoolean closed = new AtomicBoolean();
        private int markIndex = INVALID_MARK;
        private int markLimit;

        @Override
        public void close() throws IOException {
            markLimit = 0;
            markIndex = INVALID_MARK;

            if (closed.compareAndSet(false, true)) {
                final ClientFuture<Void> closed = receiver.session().getFutureFactory().createFuture();

                try {
                    executor.execute(() -> {
                        autoAcceptDeliveryIfNecessary();

                        // If the deliver wasn't fully read either because there are remaining
                        // bytes locally we need to discard those to aid in retention avoidance.
                        // and to potentially open the session window to allow for fully reading
                        // and discarding any inbound bytes that remain.
                        protonDelivery.readAll();

                        // Clear anything that wasn't yet read and then clear any pending read request as EOF
                        buffer.setIndex(buffer.capacity(), buffer.capacity());
                        buffer.reclaimRead();

                        if (readRequest != null) {
                            readRequest.complete(-1);
                            readRequest = null;
                        }

                        closed.complete(null);
                    });

                    receiver.session().request(receiver, closed);
                } catch (Exception error) {
                    LOG.debug("Ignoring error on RawInputStream close: ", error);
                } finally {
                    super.close();
                }
            }
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public synchronized void mark(int readlimit) {
            markIndex = buffer.getReadIndex();
            markLimit = readlimit;
        }

        @Override
        public synchronized void reset() throws IOException {
            if (markIndex != INVALID_MARK) {
                buffer.setReadIndex(markIndex);

                markIndex = INVALID_MARK;
                markLimit = 0;
            }
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
                    result = buffer.readByte() & 0xff;
                    tryReleaseReadBuffers();
                    break;
                } else if (requestMoreData() < 0) {
                    break;
                }
            }

            return result;
        }

        @Override
        public int read(byte target[], int offset, int length) throws IOException {
            checkStreamStateIsValid();

            Objects.checkFromIndexSize(offset, length, target.length);

            int remaining = length;
            int bytesRead = 0;

            if (length <= 0) {
                return 0;
            }

            while (remaining > 0) {
                if (buffer.isReadable()) {
                    if (buffer.getReadableBytes() < remaining) {
                        final int readTarget = buffer.getReadableBytes();
                        buffer.readBytes(target, offset + bytesRead, buffer.getReadableBytes());
                        bytesRead += readTarget;
                        remaining -= readTarget;
                    } else {
                        buffer.readBytes(target, offset + bytesRead, remaining);
                        bytesRead += remaining;
                        remaining = 0;
                    }

                    tryReleaseReadBuffers();
                } else if (requestMoreData() < 0) {
                    return bytesRead > 0 ? bytesRead : -1;
                }
            }

            return bytesRead;
        }

        @Override
        public long skip(long amount) throws IOException {
            checkStreamStateIsValid();

            long remaining = amount;

            if (amount <= 0) {
                return 0;
            }

            while (remaining > 0) {
                if (buffer.isReadable()) {
                    if (buffer.getReadableBytes() < remaining) {
                        remaining -= buffer.getReadableBytes();
                        buffer.skipBytes(buffer.getReadableBytes());
                    } else {
                        buffer.skipBytes((int) remaining);
                        remaining = 0;
                    }

                    tryReleaseReadBuffers();
                } else if (requestMoreData() < 0) {
                    break;
                }
            }

            return amount - remaining;
        }

        @Override
        public long transferTo(OutputStream target) throws IOException {
            checkStreamStateIsValid();
            // TODO: Implement efficient read and forward without intermediate copies
            //       from the currently available buffer to the output stream.
            return super.transferTo(target);
        }

        private void tryReleaseReadBuffers() {
            if (buffer.getReadIndex() - markIndex > markLimit) {
                markIndex = INVALID_MARK;
                markLimit = 0;
                buffer.reclaimRead();
            }
        }

        private void handleDeliveryRead(IncomingDelivery delivery) {
            if (closed.get()) {
                // Clear any pending data to expand session window if not yet complete
                delivery.readAll();
            } else {
                // An input stream is awaiting some more incoming bytes, check to see if
                // the delivery had a non-empty transfer frame and provide them.
                if (readRequest != null) {
                    if (delivery.available() > 0) {
                        buffer.append(protonDelivery.readAll());
                        readRequest.complete(buffer.getReadableBytes());
                    } else if (!delivery.isPartial()) {
                        autoAcceptDeliveryIfNecessary();
                        readRequest.complete(-1);
                    }

                    readRequest = null;
                }
            }
        }

        private void handleDeliveryAborted(IncomingDelivery delivery) {
            if (readRequest != null) {
                readRequest.failed(new ClientDeliveryAbortedException("The remote sender has aborted this delivery"));
            }

            delivery.settle();
        }

        private void handleReceiverClosed(ClientStreamReceiver receiver) {
            if (readRequest != null) {
                readRequest.failed(new ClientResourceRemotelyClosedException("The receliver link has been remotely closed."));
            }
        }

        private int requestMoreData() throws IOException {
            final ClientFuture<Integer> request = receiver.session().getFutureFactory().createFuture();

            try {
                executor.execute(() -> {
                    if (protonDelivery.getLink().isLocallyClosed()) {
                        request.complete(-1);
                    } else if (protonDelivery.available() > 0) {
                        buffer.append(protonDelivery.readAll());
                        request.complete(buffer.getReadableBytes());
                    } else if (protonDelivery.isAborted()) {
                        request.failed(new ClientDeliveryAbortedException("The remote sender has aborted this delivery"));
                    } else if (!protonDelivery.isPartial()) {
                        autoAcceptDeliveryIfNecessary();
                        request.complete(-1);
                    } else {
                        readRequest = request;
                    }
                });

                return receiver.session().request(receiver, request);
            } catch (Exception e) {
                throw new IOException("Error reading requested data", e);
            }
        }

        private void autoAcceptDeliveryIfNecessary() {
            if (receiver.receiverOptions().autoAccept() && !protonDelivery.isSettled()) {
                if (!buffer.isReadable() && protonDelivery.available() == 0 &&
                    (protonDelivery.isAborted() || !protonDelivery.isPartial())) {

                    try {
                        receiver.disposition(protonDelivery, Accepted.getInstance(), receiver.receiverOptions().autoSettle());
                    } catch (Exception error) {
                        LOG.trace("Caught error while attempting to auto accept the fully read delivery.", error);
                    }
                }
            }
        }

        private void checkStreamStateIsValid() throws IOException {
            if (closed.get()) {
                throw new IOException("The InputStream has been explicity closed");
            }

            if (receiver.isClosed()) {
                throw new IOException("Underlying receiver has closed", receiver.getFailureCause());
            }
        }
    }
}
