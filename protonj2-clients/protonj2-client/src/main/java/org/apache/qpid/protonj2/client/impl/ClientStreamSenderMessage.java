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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.OutputStreamOptions;
import org.apache.qpid.protonj2.client.StreamSenderMessage;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Streaming Sender context used to multiple send operations that comprise the payload
 * of a single larger message transfer.
 */
public class ClientStreamSenderMessage implements StreamSenderMessage {

    private static final int DATA_SECTION_HEADER_ENCODING_SIZE = 8;

    private final ClientStreamSender sender;
    private final ClientStreamTracker tracker;
    private final OutgoingDelivery protonDelivery;
    private final int writeBufferSize;
    private final SendContextMessage contextMessage = new SendContextMessage();

    private ProtonBuffer buffer;
    private volatile int messageFormat;
    private volatile boolean completed;
    private volatile boolean aborted;
    private boolean active;

    ClientStreamSenderMessage(ClientStreamSender sender, OutgoingDelivery protonDelivery) {
        this.sender = sender;
        this.protonDelivery = protonDelivery;
        this.tracker = new ClientStreamTracker(this);

        if (sender.options().writeBufferSize() > 0) {
            writeBufferSize = Math.max(StreamSenderOptions.MIN_BUFFER_SIZE_LIMIT, sender.options().writeBufferSize());
        } else {
            writeBufferSize = Math.max(StreamSenderOptions.MIN_BUFFER_SIZE_LIMIT,
                                  (int) sender.getProtonSender().getConnection().getMaxFrameSize());
        }
    }

    @Override
    public ClientStreamSender sender() {
        return sender;
    }

    @Override
    public ClientStreamTracker tracker() {
        return tracker;
    }

    @Override
    public int messageFormat() throws ClientException {
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
    public OutputStream body(OutputStreamOptions options) throws ClientException {
        if (completed()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a completed send context");
        }

        if (aborted()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a aborted send context");
        }

        ProtonBuffer streamBuffer = ProtonByteBufferAllocator.DEFAULT.allocate(writeBufferSize, writeBufferSize);

        if (options.streamSize() > 0) {
            return new SendContextSingularDataSectionOutputStream(options, streamBuffer);
        } else {
            return new SendContextContiguousDataSectionsOutputStream(options, streamBuffer);
        }
    }

    @Override
    public OutputStream rawOutputStream() throws ClientException {
        if (completed()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a completed send context");
        }

        if (aborted()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a aborted send context");
        }

        return new SendContextRawBytesOutputStream(ProtonByteBufferAllocator.DEFAULT.allocate(writeBufferSize, writeBufferSize));
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

        if (buffer.getReadableBytes() >= writeBufferSize) {
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
                    tracker().settlementFuture().get();
                    tracker().settle();
                } else {
                    streamBuffer.setIndex(0, 0);
                }
            } catch (ClientException | InterruptedException | ExecutionException e) {
                new IOException(e);
            }
        }
    }

    private final class SendContextRawBytesOutputStream extends SendContextOutputStream {

        public SendContextRawBytesOutputStream(ProtonBuffer buffer) {
            super(new OutputStreamOptions(), buffer);
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

    @Override
    public boolean durable() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Message<OutputStream> durable(boolean durable) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte priority() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<OutputStream> priority(byte priority) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long timeToLive() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<OutputStream> timeToLive(long timeToLive) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean firstAcquirer() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Message<OutputStream> firstAcquirer(boolean firstAcquirer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long deliveryCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<OutputStream> deliveryCount(long deliveryCount) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object messageId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> messageId(Object messageId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] userId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> userId(byte[] userId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String to() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> to(String to) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String subject() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> subject(String subject) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String replyTo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> replyTo(String replyTo) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object correlationId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> correlationId(Object correlationId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String contentType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> contentType(String contentType) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String contentEncoding() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<?> contentEncoding(String contentEncoding) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long absoluteExpiryTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<OutputStream> absoluteExpiryTime(long expiryTime) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long creationTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<OutputStream> creationTime(long createTime) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String groupId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> groupId(String groupId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int groupSequence() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<OutputStream> groupSequence(int groupSequence) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String replyToGroupId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> replyToGroupId(String replyToGroupId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object messageAnnotation(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasMessageAnnotation(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasMessageAnnotations() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object removeMessageAnnotation(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> forEachMessageAnnotation(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> messageAnnotation(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object applicationProperty(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasApplicationProperty(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasApplicationProperties() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object removeApplicationProperty(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> forEachApplicationProperty(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> applicationProperty(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object footer(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasFooter(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasFooters() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object removeFooter(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> forEachFooter(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> footer(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OutputStream body() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<OutputStream> body(OutputStream value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Header header() throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> header(Header header) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MessageAnnotations annotations() throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> annotations(MessageAnnotations messageAnnotations) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties properties() throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> properties(Properties properties) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ApplicationProperties applicationProperties() throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> applicationProperties(ApplicationProperties applicationProperties) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Footer footer() throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> footer(Footer footer) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> addBodySection(Section<?> bodySection) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> bodySections(Collection<Section<?>> sections) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<Section<?>> bodySections() throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> forEachBodySection(Consumer<Section<?>> consumer) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<OutputStream> clearBodySections() throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer encode(Map<String, Object> deliveryAnnotations) throws ClientException {
        // TODO Auto-generated method stub
        return null;
    }

    OutgoingDelivery protonDelivery() {
        return protonDelivery;
    }
}
