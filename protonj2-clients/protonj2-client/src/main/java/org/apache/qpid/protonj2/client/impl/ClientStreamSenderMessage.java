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
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.OutputStreamOptions;
import org.apache.qpid.protonj2.client.StreamSenderMessage;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientOperationTimedOutException;
import org.apache.qpid.protonj2.client.futures.ClientFuture;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Streaming Sender context used to multiple send operations that comprise the payload
 * of a single larger message transfer.
 */
public class ClientStreamSenderMessage implements StreamSenderMessage {

    private static final int DATA_SECTION_HEADER_ENCODING_SIZE = 8;

    private final ClientStreamSender sender;
    private final OutgoingDelivery protonDelivery;
    private final int bufferSize;
    private final SendContextMessage contextMessage = new SendContextMessage();
    private final ClientFuture<StreamSenderMessage> acknowledged;

    private ProtonBuffer buffer;
    private volatile int messageFormat;
    private volatile boolean completed;
    private volatile boolean aborted;
    private boolean active;
    private volatile boolean remotelySetted;
    private volatile DeliveryState remoteDeliveryState;

    ClientStreamSenderMessage(ClientStreamSender sender, OutgoingDelivery protonDelivery) {
        this.sender = sender;
        this.protonDelivery = protonDelivery;
        this.protonDelivery.setLinkedResource(this);
        this.acknowledged = sender.session().getFutureFactory().createFuture();

        if (sender.options().writeBufferSize() > 0) {
            bufferSize = Math.max(StreamSenderOptions.MIN_BUFFER_SIZE_LIMIT, sender.options().writeBufferSize());
        } else {
            bufferSize = Math.max(StreamSenderOptions.MIN_BUFFER_SIZE_LIMIT,
                                  (int) sender.getProtonSender().getConnection().getMaxFrameSize());
        }
    }

    @Override
    public ClientStreamSender sender() {
        return sender;
    }

    @Override
    public int messageFormat() {
        return messageFormat;
    }

    @Override
    public ClientStreamSenderMessage messageFormat(int messageFormat) throws ClientException {
        if (active) {
            throw new ClientIllegalStateException("Cannot set message format after writes have started.");
        }

        this.messageFormat = messageFormat;

        return this;
    }

    @Override
    public ClientStreamSenderMessage write(Section<?> section) throws ClientException {
        if (aborted()) {
            throw new ClientIllegalStateException("Cannot write a Section to an already aborted send context");
        }

        if (completed()) {
            throw new ClientIllegalStateException("Cannot write a Section to an already completed send context");
        }

        appenedDataToBuffer(ClientMessageSupport.encodeSection(section, ProtonByteBufferAllocator.DEFAULT.allocate()));

        return this;
    }

    @Override
    public ClientStreamSenderMessage flush() throws ClientException {
        if (aborted()) {
            throw new ClientIllegalStateException("Cannot flush already aborted send context");
        }

        if (completed()) {
            throw new ClientIllegalStateException("Cannot flush an already completed send context");
        }

        doFlush();

        return this;
    }

    private void doFlush() throws ClientException {
        if (buffer != null && buffer.isReadable()) {
            try {
                sender.sendMessage(this, contextMessage);
            } finally {
                active = true;
                buffer = null;
            }
        }
    }

    @Override
    public ClientStreamSenderMessage abort() throws ClientException {
        if (completed()) {
            throw new ClientIllegalStateException("Cannot abort an already completed send context");
        }

        if (!aborted()) {
            aborted = true;

            if (active) {
                sender.abort(protonDelivery);
            }
        }

        return this;
    }

    @Override
    public boolean aborted() {
        return aborted;
    }

    @Override
    public ClientStreamSenderMessage complete() throws ClientException {
        if (aborted()) {
            throw new ClientIllegalStateException("Cannot complete an already aborted send context");
        }

        if (!completed()) {
            completed = true;

            // If there is buffered data we can flush and complete in one Transfer
            // frame otherwise we only need to do work if there was ever a send on
            // this context which would imply we have a Tracker.
            if (buffer != null && buffer.isReadable()) {
                doFlush();
            } else if (active) {
                sender.complete(protonDelivery);
            }
        }

        return this;
    }

    @Override
    public boolean completed() {
        return completed;
    }

    @Override
    public OutputStream dataOutputStream(OutputStreamOptions options) throws ClientException {
        if (completed()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a completed send context");
        }

        if (aborted()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a aborted send context");
        }

        ProtonBuffer streamBuffer = ProtonByteBufferAllocator.DEFAULT.allocate(bufferSize, bufferSize);

        if (options.streamSize() > 0) {
            return new SendContextSingularDataSectionOutputStream(options, streamBuffer);
        } else {
            return new SendContextContiguousDataSectionsOutputStream(options, streamBuffer);
        }
    }

    @Override
    public OutputStream rawOutputStream(OutputStreamOptions options) throws ClientException {
        if (completed()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a completed send context");
        }

        if (aborted()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a aborted send context");
        }

        flush();

        return new SendContextRawBytesOutputStream(options, ProtonByteBufferAllocator.DEFAULT.allocate(bufferSize, bufferSize));
    }

    @Override
    public DeliveryState state() {
        return ClientDeliveryState.fromProtonType(protonDelivery.getState());
    }

    @Override
    public DeliveryState remoteState() {
        return remoteDeliveryState;
    }

    @Override
    public boolean remoteSettled() {
        return remotelySetted;
    }

    @Override
    public ClientStreamSenderMessage disposition(DeliveryState state, boolean settle) throws ClientException {
        org.apache.qpid.protonj2.types.transport.DeliveryState protonState = null;
        if (state != null) {
            protonState = ClientDeliveryState.asProtonType(state);
        }

        sender.disposition(protonDelivery, protonState, settle);
        return this;
    }

    @Override
    public ClientStreamSenderMessage settle() throws ClientException {
        sender.disposition(protonDelivery, null, true);
        return this;
    }

    @Override
    public boolean settled() {
        return protonDelivery.isSettled();
    }

    @Override
    public ClientFuture<StreamSenderMessage> settlementFuture() {
        return acknowledged;
    }

    @Override
    public StreamSenderMessage awaitSettlement() throws ClientException {
        try {
            return settlementFuture().get();
        } catch (ExecutionException exe) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(exe.getCause());
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ClientException("Wait for settlement was interrupted", e);
        }
    }

    @Override
    public StreamSenderMessage awaitSettlement(long timeout, TimeUnit unit) throws ClientException {
        try {
            return settlementFuture().get(timeout, unit);
        } catch (InterruptedException ie) {
            Thread.interrupted();
            throw new ClientException("Wait for settlement was interrupted", ie);
        } catch (ExecutionException exe) {
            throw ClientExceptionSupport.createNonFatalOrPassthrough(exe.getCause());
        } catch (TimeoutException te) {
            throw new ClientOperationTimedOutException("Timed out waiting for remote settlement", te);
        }
    }

    private void appenedDataToBuffer(ProtonBuffer incoming) throws ClientException {
        if (buffer == null) {
            buffer = incoming;
        } else {
            if (buffer instanceof ProtonCompositeBuffer) {
                ((ProtonCompositeBuffer) buffer).append(incoming);
            } else {
                ProtonCompositeBuffer composite = new ProtonCompositeBuffer();
                composite.append(buffer).append(incoming);

                buffer = composite;
            }
        }

        if (buffer.getReadableBytes() >= bufferSize) {
            try {
                sender.sendMessage(this, contextMessage);
            } finally {
                active = true;
                buffer = null;
            }
        }
    }

    private final class SendContextMessage extends ClientMessage<byte[]> {

        @Override
        public int messageFormat() {
            return messageFormat;
        }

        @Override
        public ProtonBuffer encode(Map<String, Object> deliveryAnnotations) {
            return buffer;
        }
    }

    //----- OutputStream implementation for the Send Context

    private abstract class SendContextOutputStream extends OutputStream {

        protected final AtomicBoolean closed = new AtomicBoolean();
        protected final OutputStreamOptions options;
        protected final ProtonBuffer streamBuffer;

        protected int bytesWritten;

        public SendContextOutputStream(OutputStreamOptions options, ProtonBuffer buffer) {
            this.options = options;
            this.streamBuffer = buffer;
        }

        @Override
        public void write(int value) throws IOException {
            checkClosed();
            checkOutputLimitReached(1);
            streamBuffer.writeByte(value);
            if (!streamBuffer.isWritable()) {
                flush();
            }
            bytesWritten++;
        }

        @Override
        public void write(byte bytes[]) throws IOException {
            write(bytes, 0, bytes.length);
        }

        @Override
        public void write(byte bytes[], int offset, int length) throws IOException {
            checkClosed();
            checkOutputLimitReached(length);
            if (streamBuffer.getWritableBytes() >= length) {
                streamBuffer.writeBytes(bytes, offset, length);
                bytesWritten += length;
                if (!streamBuffer.isWritable()) {
                    flush();
                }
            } else {
                int remaining = length;

                while (remaining > 0) {
                    int toWrite = Math.min(length, streamBuffer.getWritableBytes());
                    bytesWritten += toWrite;
                    streamBuffer.writeBytes(bytes, offset + (length - remaining), toWrite);
                    if (!streamBuffer.isWritable()) {
                        flush();
                    }
                    remaining -= toWrite;
                }
            }
        }

        @Override
        public void flush() throws IOException {
            checkClosed();

            if (streamBuffer.getReadableBytes() > 0) {
                doFlushPending(bytesWritten == options.streamSize() && options.completeSendOnClose());
            }
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true) && bytesWritten > 0 && !completed()) {
                if (options.streamSize() > 0 && options.streamSize() != bytesWritten) {
                    // Limit was set but user did not write all of it so we must abort.
                    try {
                        abort();
                    } catch (ClientException e) {
                        throw new IOException(e);
                    }
                } else {
                    // Limit not set or was set and user wrote that many bytes so we can complete.
                    doFlushPending(options.completeSendOnClose());
                }
            }
        }

        private void checkOutputLimitReached(int writeSize) throws IOException {
            final int outputLimit = options.streamSize();

            if (completed()) {
                throw new IOException("Cannot write to an already completed message output stream");
            }

            if (outputLimit > 0 && (bytesWritten + writeSize) > outputLimit) {
                throw new IOException("Cannot write beyond configured stream output limit");
            }
        }

        private void checkClosed() throws IOException {
            if (closed.get()) {
                throw new IOException("The OutputStream has already been closed.");
            }

            if (sender.isClosed()) {
                throw new IOException("The parent Sender instance has already been closed.");
            }
        }

        protected void doFlushPending(boolean complete) throws IOException {
            try {
                if (streamBuffer.isReadable()) {
                    appenedDataToBuffer(streamBuffer);
                }

                if (complete) {
                    complete();
                } else {
                    doFlush();
                }

                if (complete) {
                    settlementFuture().get();
                    settle();
                } else {
                    streamBuffer.setIndex(0, 0);
                }
            } catch (ClientException | InterruptedException | ExecutionException e) {
                new IOException(e);
            }
        }
    }

    private final class SendContextRawBytesOutputStream extends SendContextOutputStream {

        public SendContextRawBytesOutputStream(OutputStreamOptions options, ProtonBuffer buffer) {
            super(options, buffer);
        }
    }

    private final class SendContextSingularDataSectionOutputStream extends SendContextOutputStream {

        public SendContextSingularDataSectionOutputStream(OutputStreamOptions options, ProtonBuffer buffer) throws ClientException {
            super(options, buffer);

            ProtonBuffer preamble = ProtonByteBufferAllocator.DEFAULT.allocate(DATA_SECTION_HEADER_ENCODING_SIZE, DATA_SECTION_HEADER_ENCODING_SIZE);

            preamble.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
            preamble.writeByte(EncodingCodes.SMALLULONG);
            preamble.writeByte(Data.DESCRIPTOR_CODE.byteValue());
            preamble.writeByte(EncodingCodes.VBIN32);
            preamble.writeInt(options.streamSize());

            appenedDataToBuffer(preamble);
        }
    }

    private final class SendContextContiguousDataSectionsOutputStream extends SendContextOutputStream {

        public SendContextContiguousDataSectionsOutputStream(OutputStreamOptions options, ProtonBuffer buffer) {
            super(options, buffer);
        }

        @Override
        protected void doFlushPending(boolean complete) throws IOException {
            if (streamBuffer.isReadable()) {

                ProtonBuffer preamble = ProtonByteBufferAllocator.DEFAULT.allocate(DATA_SECTION_HEADER_ENCODING_SIZE, DATA_SECTION_HEADER_ENCODING_SIZE);

                preamble.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
                preamble.writeByte(EncodingCodes.SMALLULONG);
                preamble.writeByte(Data.DESCRIPTOR_CODE.byteValue());
                preamble.writeByte(EncodingCodes.VBIN32);
                preamble.writeInt(streamBuffer.getReadableBytes());

                try {
                    appenedDataToBuffer(preamble);
                } catch (ClientException e) {
                    throw new IOException(e);
                }
            }

            super.doFlushPending(complete);
        }
    }

    //----- Internal Event hooks for delivery updates

    void processDeliveryUpdated(OutgoingDelivery delivery) {
        remotelySetted = delivery.isRemotelySettled();
        remoteDeliveryState = ClientDeliveryState.fromProtonType(delivery.getRemoteState());

        if (delivery.isRemotelySettled()) {
            acknowledged.complete(this);
        }
    }
}
