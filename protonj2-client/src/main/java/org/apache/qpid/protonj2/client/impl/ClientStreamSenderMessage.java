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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.OutputStreamOptions;
import org.apache.qpid.protonj2.client.StreamSenderMessage;
import org.apache.qpid.protonj2.client.StreamSenderOptions;
import org.apache.qpid.protonj2.client.StreamTracker;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.engine.OutgoingDelivery;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Streaming Sender context used to multiple send operations that comprise the payload
 * of a single larger message transfer.
 */
final class ClientStreamSenderMessage implements StreamSenderMessage {

    private static final int DATA_SECTION_HEADER_ENCODING_SIZE = 8;

    private enum StreamState {
        PREAMBLE,
        BODY_WRITABLE,
        BODY_WRITTING,
        COMPLETE,
        ABORTED
    }

    private final ClientStreamSender sender;
    private final DeliveryAnnotations deliveryAnnotations;
    private final int writeBufferSize;
    private final StreamMessagePacket streamMessagePacket = new StreamMessagePacket();
    private final ClientStreamTracker tracker;

    private Header header;
    private MessageAnnotations annotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Footer footer;

    private ProtonBuffer buffer;
    private volatile int messageFormat;
    private StreamState currentState = StreamState.PREAMBLE;

    ClientStreamSenderMessage(ClientStreamSender sender, ClientStreamTracker tracker, DeliveryAnnotations deliveryAnnotations) {
        this.sender = sender;
        this.deliveryAnnotations = deliveryAnnotations;
        this.tracker = tracker;

        if (sender.options().writeBufferSize() > 0) {
            writeBufferSize = Math.max(StreamSenderOptions.MIN_BUFFER_SIZE_LIMIT, sender.options().writeBufferSize());
        } else {
            writeBufferSize = Math.max(StreamSenderOptions.MIN_BUFFER_SIZE_LIMIT,
                                  (int) sender.getProtonSender().getConnection().getMaxFrameSize());
        }
    }

    OutgoingDelivery getProtonDelivery() {
        return tracker.delivery();
    }

    @Override
    public ClientStreamSender sender() {
        return sender;
    }

    @Override
    public StreamTracker tracker() {
        return completed() ? tracker : null;
    }

    @Override
    public int messageFormat() throws ClientException {
        return messageFormat;
    }

    @Override
    public ClientStreamSenderMessage messageFormat(int messageFormat) throws ClientException {
        if (currentState != StreamState.PREAMBLE) {
            throw new ClientIllegalStateException("Cannot set message format after body writes have started.");
        }

        this.messageFormat = messageFormat;

        return this;
    }

    private void doFlush() throws ClientException {
        if (buffer != null && buffer.isReadable()) {
            try {
                sender.sendMessage(this, streamMessagePacket);
            } finally {
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
            currentState = StreamState.ABORTED;
            sender.abort(getProtonDelivery(), tracker);
        }

        return this;
    }

    @Override
    public boolean aborted() {
        return currentState == StreamState.ABORTED;
    }

    @Override
    public ClientStreamSenderMessage complete() throws ClientException {
        if (aborted()) {
            throw new ClientIllegalStateException("Cannot complete an already aborted send context");
        }

        if (!completed()) {
            // This may result in completion if the write surpasses the buffer limit but we still
            // need to check in case it does not, or if there are no footers...
            if (footer != null) {
                write(footer);
            }

            currentState = StreamState.COMPLETE;

            // If there is buffered data we can flush and complete in one Transfer
            // frame otherwise we only need to do work if there was ever a send on
            // this context which would imply we have a Tracker and a Delivery.
            if (buffer != null && buffer.isReadable()) {
                doFlush();
            } else {
                sender.complete(getProtonDelivery(), tracker);
            }
        }

        return this;
    }

    @Override
    public boolean completed() {
        return currentState == StreamState.COMPLETE;
    }

    @Override
    public Message<OutputStream> body(OutputStream value) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot set an OutputStream body on a StreamSenderMessage");
    }

    @Override
    public StreamSenderMessage addBodySection(Section<?> bodySection) throws ClientException {
        if (completed()) {
            throw new ClientIllegalStateException("Cannot add more body sections to a completed message");
        }

        if (aborted()) {
            throw new ClientIllegalStateException("Cannot add more body sections to an aborted message");
        }

        if (currentState == StreamState.BODY_WRITTING) {
            throw new ClientIllegalStateException("Cannot add more body sections while an OutputStream is active");
        }

        transitionToWritableState();

        appenedDataToBuffer(ClientMessageSupport.encodeSection(bodySection, ProtonByteBufferAllocator.DEFAULT.allocate()));

        return this;
    }

    @Override
    public StreamSenderMessage bodySections(Collection<Section<?>> sections) throws ClientException {
        Objects.requireNonNull(sections, "Cannot set body sections with a null Collection");

        for (Section<?> section : sections) {
            addBodySection(section);
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<Section<?>> bodySections() throws ClientException {
        // Body sections are not held in memory but encoded as offered so they cannot be returned.
        return Collections.EMPTY_LIST;
    }

    @Override
    public StreamSenderMessage forEachBodySection(Consumer<Section<?>> consumer) throws ClientException {
        // Body sections are not held in memory but encoded as offered so they are not iterable.
        return this;
    }

    @Override
    public StreamSenderMessage clearBodySections() throws ClientException {
        // Body sections are not held in memory but encoded as offered so they cannot be cleared.
        return this;
    }

    @Override
    public OutputStream body() throws ClientException {
        return body(new OutputStreamOptions());
    }

    @Override
    public OutputStream body(OutputStreamOptions options) throws ClientException {
        if (completed()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a completed send context");
        }

        if (aborted()) {
            throw new ClientIllegalStateException("Cannot create an OutputStream from a aborted send context");
        }

        if (currentState == StreamState.BODY_WRITTING) {
            throw new ClientIllegalStateException("Cannot add more body sections while an OutputStream is active");
        }

        transitionToWritableState();

        ProtonBuffer streamBuffer = ProtonByteBufferAllocator.DEFAULT.allocate(writeBufferSize, writeBufferSize);

        if (options.bodyLength() > 0) {
            return new SingularDataSectionOutputStream(options, streamBuffer);
        } else {
            return new MultipleDataSectionsOutputStream(options, streamBuffer);
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

        if (currentState == StreamState.BODY_WRITTING) {
            throw new ClientIllegalStateException("Cannot add more body sections while an OutputStream is active");
        }

        transitionToWritableState();

        return new SendContextRawBytesOutputStream(ProtonByteBufferAllocator.DEFAULT.allocate(writeBufferSize, writeBufferSize));
    }

    //----- OutputStream implementation for the Send Context

    private abstract class StreamMessageOutputStream extends OutputStream {

        protected final AtomicBoolean closed = new AtomicBoolean();
        protected final OutputStreamOptions options;
        protected final ProtonBuffer streamBuffer;

        protected int bytesWritten;

        public StreamMessageOutputStream(OutputStreamOptions options, ProtonBuffer buffer) {
            this.options = options;
            this.streamBuffer = buffer;

            // Stream takes control of state until closed.
            currentState = StreamState.BODY_WRITTING;
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
                    int toWrite = Math.min(remaining, streamBuffer.getWritableBytes());
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

            if (options.bodyLength() <= 0) {
                doFlushPending(false);
            } else {
                doFlushPending(bytesWritten == options.bodyLength() && options.completeSendOnClose());
            }
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true) && !completed()) {
                currentState = StreamState.BODY_WRITABLE;

                if (options.bodyLength() > 0 && options.bodyLength() != bytesWritten) {
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
            final int outputLimit = options.bodyLength();

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

                if (!complete) {
                    streamBuffer.setIndex(0, 0);
                }
            } catch (ClientException e) {
                throw new IOException(e);
            }
        }
    }

    private final class SendContextRawBytesOutputStream extends StreamMessageOutputStream {

        public SendContextRawBytesOutputStream(ProtonBuffer buffer) {
            super(new OutputStreamOptions(), buffer);
        }
    }

    private final class SingularDataSectionOutputStream extends StreamMessageOutputStream {

        public SingularDataSectionOutputStream(OutputStreamOptions options, ProtonBuffer buffer) throws ClientException {
            super(options, buffer);

            ProtonBuffer preamble = ProtonByteBufferAllocator.DEFAULT.allocate(DATA_SECTION_HEADER_ENCODING_SIZE, DATA_SECTION_HEADER_ENCODING_SIZE);

            preamble.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
            preamble.writeByte(EncodingCodes.SMALLULONG);
            preamble.writeByte(Data.DESCRIPTOR_CODE.byteValue());
            preamble.writeByte(EncodingCodes.VBIN32);
            preamble.writeInt(options.bodyLength());

            appenedDataToBuffer(preamble);
        }
    }

    private final class MultipleDataSectionsOutputStream extends StreamMessageOutputStream {

        public MultipleDataSectionsOutputStream(OutputStreamOptions options, ProtonBuffer buffer) {
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

    //----- Message Header API

    @Override
    public boolean durable() {
        return header == null ? Header.DEFAULT_DURABILITY : header.isDurable();
    }

    @Override
    public StreamSenderMessage durable(boolean durable) throws ClientIllegalStateException {
        lazyCreateHeader().setDurable(durable);
        return this;
    }

    @Override
    public byte priority() {
        return header == null ? Header.DEFAULT_PRIORITY : header.getPriority();
    }

    @Override
    public StreamSenderMessage priority(byte priority) throws ClientIllegalStateException {
        lazyCreateHeader().setPriority(priority);
        return this;
    }

    @Override
    public long timeToLive() {
        return header == null ? Header.DEFAULT_TIME_TO_LIVE : header.getTimeToLive();
    }

    @Override
    public StreamSenderMessage timeToLive(long timeToLive) throws ClientIllegalStateException {
        lazyCreateHeader().setTimeToLive(timeToLive);
        return this;
    }

    @Override
    public boolean firstAcquirer() {
        return header == null ? Header.DEFAULT_FIRST_ACQUIRER : header.isFirstAcquirer();
    }

    @Override
    public StreamSenderMessage firstAcquirer(boolean firstAcquirer) throws ClientIllegalStateException {
        lazyCreateHeader().setFirstAcquirer(firstAcquirer);
        return this;
    }

    @Override
    public long deliveryCount() {
        return header == null ? Header.DEFAULT_DELIVERY_COUNT : header.getDeliveryCount();
    }

    @Override
    public StreamSenderMessage deliveryCount(long deliveryCount) throws ClientIllegalStateException {
        lazyCreateHeader().setDeliveryCount(deliveryCount);
        return this;
    }

    //----- Message Properties access

    @Override
    public Object messageId() {
        return properties != null ? properties.getMessageId() : null;
    }

    @Override
    public StreamSenderMessage messageId(Object messageId) throws ClientIllegalStateException {
        lazyCreateProperties().setMessageId(messageId);
        return this;
    }

    @Override
    public byte[] userId() {
        byte[] copyOfUserId = null;
        if (properties != null && properties.getUserId() != null) {
            copyOfUserId = properties.getUserId().arrayCopy();
        }

        return copyOfUserId;
    }

    @Override
    public StreamSenderMessage userId(byte[] userId) throws ClientIllegalStateException {
        lazyCreateProperties().setUserId(new Binary(Arrays.copyOf(userId, userId.length)));
        return this;
    }

    @Override
    public String to() {
        return properties != null ? properties.getTo() : null;
    }

    @Override
    public StreamSenderMessage to(String to) throws ClientIllegalStateException {
        lazyCreateProperties().setTo(to);
        return this;
    }

    @Override
    public String subject() {
        return properties != null ? properties.getSubject() : null;
    }

    @Override
    public StreamSenderMessage subject(String subject) throws ClientIllegalStateException {
        lazyCreateProperties().setSubject(subject);
        return this;
    }

    @Override
    public String replyTo() {
        return properties != null ? properties.getReplyTo() : null;
    }

    @Override
    public StreamSenderMessage replyTo(String replyTo) throws ClientIllegalStateException {
        lazyCreateProperties().setReplyTo(replyTo);
        return this;
    }

    @Override
    public Object correlationId() {
        return properties != null ? properties.getCorrelationId() : null;
    }

    @Override
    public StreamSenderMessage correlationId(Object correlationId) throws ClientIllegalStateException {
        lazyCreateProperties().setCorrelationId(correlationId);
        return this;
    }

    @Override
    public String contentType() {
        return properties != null ? properties.getContentType() : null;
    }

    @Override
    public StreamSenderMessage contentType(String contentType) throws ClientIllegalStateException {
        lazyCreateProperties().setContentType(contentType);
        return this;
    }

    @Override
    public String contentEncoding() {
        return properties != null ? properties.getContentEncoding() : null;
    }

    @Override
    public StreamSenderMessage contentEncoding(String contentEncoding) throws ClientIllegalStateException {
        lazyCreateProperties().setContentEncoding(contentEncoding);
        return this;
    }

    @Override
    public long absoluteExpiryTime() {
        return properties != null ? properties.getAbsoluteExpiryTime() : 0;
    }

    @Override
    public StreamSenderMessage absoluteExpiryTime(long expiryTime) throws ClientIllegalStateException {
        lazyCreateProperties().setAbsoluteExpiryTime(expiryTime);
        return this;
    }

    @Override
    public long creationTime() {
        return properties != null ? properties.getCreationTime() : 0;
    }

    @Override
    public StreamSenderMessage creationTime(long createTime) throws ClientIllegalStateException {
        lazyCreateProperties().setCreationTime(createTime);
        return this;
    }

    @Override
    public String groupId() {
        return properties != null ? properties.getGroupId() : null;
    }

    @Override
    public StreamSenderMessage groupId(String groupId) throws ClientIllegalStateException {
        lazyCreateProperties().setGroupId(groupId);
        return this;
    }

    @Override
    public int groupSequence() {
        return properties != null ? (int) properties.getGroupSequence() : 0;
    }

    @Override
    public StreamSenderMessage groupSequence(int groupSequence) throws ClientIllegalStateException {
        lazyCreateProperties().setGroupSequence(groupSequence);
        return this;
    }

    @Override
    public String replyToGroupId() {
        return properties != null ? properties.getReplyToGroupId() : null;
    }

    @Override
    public StreamSenderMessage replyToGroupId(String replyToGroupId) throws ClientIllegalStateException {
        lazyCreateProperties().setReplyToGroupId(replyToGroupId);
        return this;
    }

    //----- Message Annotations Access

    @Override
    public Object annotation(String key) {
        Object value = null;
        if (annotations != null) {
            value = annotations.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    @Override
    public boolean hasAnnotation(String key) {
        if (annotations != null && annotations.getValue() != null) {
            return annotations.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    @Override
    public boolean hasAnnotations() {
        return annotations != null &&
               annotations.getValue() != null &&
               annotations.getValue().size() > 0;
    }

    @Override
    public Object removeAnnotation(String key) {
        if (hasAnnotations()) {
            return annotations.getValue().remove(Symbol.valueOf(key));
        } else {
            return null;
        }
     }

    @Override
    public StreamSenderMessage forEachAnnotation(BiConsumer<String, Object> action) {
        if (hasAnnotations()) {
            annotations.getValue().forEach((key, value) -> {
                action.accept(key.toString(), value);
            });
        }

        return this;
    }

    @Override
    public ClientStreamSenderMessage annotation(String key, Object value) throws ClientIllegalStateException {
        lazyCreateMessageAnnotations().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Application Properties Access

    @Override
    public Object applicationProperty(String key) {
        Object value = null;
        if (hasApplicationProperties()) {
            value = applicationProperties.getValue().get(key);
        }

        return value;
    }

    @Override
    public boolean hasApplicationProperty(String key) {
        if (hasApplicationProperties()) {
            return applicationProperties.getValue().containsKey(key);
        } else {
            return false;
        }
    }

    @Override
    public boolean hasApplicationProperties() {
        return applicationProperties != null &&
               applicationProperties.getValue() != null &&
               applicationProperties.getValue().size() > 0;
    }

    @Override
    public Object removeApplicationProperty(String key) {
        if (hasApplicationProperties()) {
            return applicationProperties.getValue().remove(key);
        } else {
            return null;
        }
     }

    @Override
    public StreamSenderMessage forEachApplicationProperty(BiConsumer<String, Object> action) {
        if (hasApplicationProperties()) {
            applicationProperties.getValue().forEach(action);
        }

        return this;
    }

    @Override
    public ClientStreamSenderMessage applicationProperty(String key, Object value) throws ClientIllegalStateException {
        lazyCreateApplicationProperties().getValue().put(key,value);
        return this;
    }

    //----- Footer Access

    @Override
    public Object footer(String key) {
        Object value = null;
        if (hasFooters()) {
            value = footer.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    @Override
    public boolean hasFooter(String key) {
        if (hasFooters()) {
            return footer.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    @Override
    public boolean hasFooters() {
        return footer != null &&
               footer.getValue() != null &&
               footer.getValue().size() > 0;
    }

    @Override
    public Object removeFooter(String key) {
        if (hasFooters()) {
            return footer.getValue().remove(Symbol.valueOf(key));
        } else {
            return null;
        }
     }

    @Override
    public StreamSenderMessage forEachFooter(BiConsumer<String, Object> action) {
        if (hasFooters()) {
            footer.getValue().forEach((key, value) -> {
                action.accept(key.toString(), value);
            });
        }

        return this;
    }

    @Override
    public ClientStreamSenderMessage footer(String key, Object value) throws ClientIllegalStateException {
        lazyCreateFooter().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- AdvancedMessage API

    @Override
    public Header header() throws ClientException {
        return header;
    }

    @Override
    public StreamSenderMessage header(Header header) throws ClientException {
        checkStreamState(StreamState.PREAMBLE, "Cannot write to Message Header after body writing has started.");
        this.header = header;
        return this;
    }

    @Override
    public MessageAnnotations annotations() throws ClientException {
        return annotations;
    }

    @Override
    public StreamSenderMessage annotations(MessageAnnotations annotations) throws ClientException {
        checkStreamState(StreamState.PREAMBLE, "Cannot write to Message Annotations after body writing has started.");
        this.annotations = annotations;
        return this;
    }

    @Override
    public Properties properties() throws ClientException {
        return properties;
    }

    @Override
    public StreamSenderMessage properties(Properties properties) throws ClientException {
        checkStreamState(StreamState.PREAMBLE, "Cannot write to Message Properties after body writing has started.");
        this.properties = properties;
        return this;
    }

    @Override
    public ApplicationProperties applicationProperties() throws ClientException {
        return applicationProperties;
    }

    @Override
    public StreamSenderMessage applicationProperties(ApplicationProperties applicationProperties) throws ClientException {
        checkStreamState(StreamState.PREAMBLE, "Cannot write to Message Application Properties after body writing has started.");
        this.applicationProperties = applicationProperties;
        return this;
    }

    @Override
    public Footer footer() throws ClientException {
        return footer;
    }

    @Override
    public StreamSenderMessage footer(Footer footer) throws ClientException {
        if (currentState.ordinal() >= StreamState.COMPLETE.ordinal()) {
            throw new ClientIllegalStateException(
                "Cannot write to Message Footer after message has been marked completed or aborted.");
        }
        this.footer = footer;
        return this;
    }

    @Override
    public ProtonBuffer encode(Map<String, Object> deliveryAnnotations) throws ClientException {
        throw new ClientUnsupportedOperationException("StreamSenderMessage cannot be directly encoded");
    }

    //----- Internal API

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

        // Were aren't currently attempting to optimize each outbound chunk of the streaming
        // send, if the block accumulated is larger than the write buffer we don't try and
        // split it but instead let the frame writer just write multiple frames.  This can
        // result in a trailing single tiny frame but for now this case isn't being optimized

        if (buffer.getReadableBytes() >= writeBufferSize) {
            try {
                sender.sendMessage(this, streamMessagePacket);
            } finally {
                buffer = null;
            }
        }
    }

    private final class StreamMessagePacket extends ClientMessage<byte[]> {

        @Override
        public int messageFormat() {
            return messageFormat;
        }

        @Override
        public ProtonBuffer encode(Map<String, Object> deliveryAnnotations) {
            return buffer;
        }
    }

    private void transitionToWritableState() throws ClientException {
        if (currentState == StreamState.PREAMBLE) {

            if (header != null) {
                appenedDataToBuffer(ClientMessageSupport.encodeSection(header, ProtonByteBufferAllocator.DEFAULT.allocate()));
            }
            if (deliveryAnnotations != null) {
                appenedDataToBuffer(ClientMessageSupport.encodeSection(deliveryAnnotations, ProtonByteBufferAllocator.DEFAULT.allocate()));
            }
            if (annotations != null) {
                appenedDataToBuffer(ClientMessageSupport.encodeSection(annotations, ProtonByteBufferAllocator.DEFAULT.allocate()));
            }
            if (properties != null) {
                appenedDataToBuffer(ClientMessageSupport.encodeSection(properties, ProtonByteBufferAllocator.DEFAULT.allocate()));
            }
            if (applicationProperties != null) {
                appenedDataToBuffer(ClientMessageSupport.encodeSection(applicationProperties, ProtonByteBufferAllocator.DEFAULT.allocate()));
            }

            currentState = StreamState.BODY_WRITABLE;
        }
    }

    private ClientStreamSenderMessage write(Section<?> section) throws ClientException {
        if (aborted()) {
            throw new ClientIllegalStateException("Cannot write a Section to an already aborted send context");
        }

        if (completed()) {
            throw new ClientIllegalStateException("Cannot write a Section to an already completed send context");
        }

        appenedDataToBuffer(ClientMessageSupport.encodeSection(section, ProtonByteBufferAllocator.DEFAULT.allocate()));

        return this;
    }

    private void checkStreamState(StreamState state, String errorMessage) throws ClientIllegalStateException {
        if (currentState != state) {
            throw new ClientIllegalStateException(errorMessage);
        }
    }

    private Header lazyCreateHeader() throws ClientIllegalStateException {
        checkStreamState(StreamState.PREAMBLE, "Cannot write to Message Header after body writing has started.");

        if (header == null) {
            header = new Header();
        }

        return header;
    }

    private Properties lazyCreateProperties() throws ClientIllegalStateException {
        checkStreamState(StreamState.PREAMBLE, "Cannot write to Message Properties after body writing has started.");

        if (properties == null) {
            properties = new Properties();
        }

        return properties;
    }

    private ApplicationProperties lazyCreateApplicationProperties() throws ClientIllegalStateException {
        checkStreamState(StreamState.PREAMBLE, "Cannot write to Message Application Properties after body writing has started.");

        if (applicationProperties == null) {
            applicationProperties = new ApplicationProperties(new LinkedHashMap<>());
        }

        return applicationProperties;
    }

    private MessageAnnotations lazyCreateMessageAnnotations() throws ClientIllegalStateException {
        checkStreamState(StreamState.PREAMBLE, "Cannot write to Message Annotations after body writing has started.");

        if (annotations == null) {
            annotations = new MessageAnnotations(new LinkedHashMap<>());
        }

        return annotations;
    }

    private Footer lazyCreateFooter() throws ClientIllegalStateException {
        if (currentState.ordinal() >= StreamState.COMPLETE.ordinal()) {
            throw new ClientIllegalStateException(
                "Cannot write to Message Footer after message has been marked completed or aborted.");
        }

        if (footer == null) {
            footer = new Footer(new LinkedHashMap<>());
        }

        return footer;
    }
}
