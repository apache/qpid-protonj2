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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.StreamReceiverMessage;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamDecoderFactory;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Streamed message delivery context used to request reads of possible split framed
 * {@link Transfer} payload's that comprise a single large overall message.
 */
public class ClientStreamReceiverMessage implements StreamReceiverMessage {

    private enum StreamState {
        IDLE,
        HEADER_READ,
        DELIVERY_ANNOTATIONS_READ,
        MESSAGE_ANNOTATIONS_READ,
        PROPERTIES_READ,
        APPLICATION_PROPERTIES_READ,
        BODY_READABLE,
        BODY_READING,
        BODY_READ,
        FOOTER_READ
    }

    private final ClientStreamReceiver receiver;
    private final ClientStreamDelivery delivery;
    private final InputStream deliveryStream;
    private final IncomingDelivery protonDelivery;
    private final StreamDecoder protonDecoder = ProtonStreamDecoderFactory.create();
    private final StreamDecoderState decoderState = protonDecoder.newDecoderState();
    private final ScheduledExecutorService executor;

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations annotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Footer footer;
    private long bodySize = 0;

    private StreamState currentState = StreamState.IDLE;
    private MessageBodyInputStream bodyStream;

    ClientStreamReceiverMessage(ClientStreamReceiver receiver, ClientStreamDelivery delivery, InputStream deliveryStream) {
        this.receiver = receiver;
        this.delivery = delivery;
        this.deliveryStream = deliveryStream;
        this.protonDelivery = delivery.getProtonDelivery();
        this.executor = receiver.session().getScheduler();
    }

    @Override
    public ClientStreamReceiver receiver() {
        return receiver;
    }

    @Override
    public ClientStreamDelivery delivery() {
        return delivery;
    }

    IncomingDelivery protonDelivery() {
        return protonDelivery;
    }

    @Override
    public boolean aborted() {
        if (protonDelivery != null) {
            return protonDelivery.isAborted();
        } else {
            return false;
        }
    }

    @Override
    public boolean completed() {
        if (protonDelivery != null) {
            return !protonDelivery.isPartial() && !protonDelivery.isAborted();
        } else {
            return false;
        }
    }

    @Override
    public int messageFormat() throws ClientException {
        return protonDelivery != null ? protonDelivery.getMessageFormat() : 0;
    }

    @Override
    public StreamReceiverMessage messageFormat(int messageFormat) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiverMessage");
    }

    //----- Header API implementation

    @Override
    public boolean durable() throws ClientException {
        return header() != null ? header.isDurable() : false;
    }

    @Override
    public StreamReceiverMessage durable(boolean durable) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public byte priority() throws ClientException {
        return header() != null ? header.getPriority() : Header.DEFAULT_PRIORITY;
    }

    @Override
    public StreamReceiverMessage priority(byte priority) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public long timeToLive() throws ClientException {
        return header() != null ? header.getTimeToLive() : Header.DEFAULT_TIME_TO_LIVE;
    }

    @Override
    public StreamReceiverMessage timeToLive(long timeToLive) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public boolean firstAcquirer() throws ClientException {
        return header() != null ? header.isFirstAcquirer() : Header.DEFAULT_FIRST_ACQUIRER;
    }

    @Override
    public StreamReceiverMessage firstAcquirer(boolean firstAcquirer) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public long deliveryCount() throws ClientException {
        return header() != null ? header.getDeliveryCount() : Header.DEFAULT_DELIVERY_COUNT;
    }

    @Override
    public StreamReceiverMessage deliveryCount(long deliveryCount) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public Header header() throws ClientException {
        ensureStreamDecodedTo(StreamState.HEADER_READ);
        return header;
    }

    @Override
    public StreamReceiverMessage header(Header header) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Properties API implementation

    @Override
    public Object messageId() throws ClientException {
        if (properties() != null) {
            return properties().getMessageId();
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage messageId(Object messageId) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public byte[] userId() throws ClientException {
        if (properties() != null) {
            byte[] copyOfUserId = null;
            if (properties != null && properties().getUserId() != null) {
                copyOfUserId = properties().getUserId().arrayCopy();
            }

            return copyOfUserId;
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage userId(byte[] userId) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public String to() throws ClientException {
        if (properties() != null) {
            return properties().getTo();
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage to(String to) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public String subject() throws ClientException {
        if (properties() != null) {
            return properties().getSubject();
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage subject(String subject) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public String replyTo() throws ClientException {
        if (properties() != null) {
            return properties().getReplyTo();
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage replyTo(String replyTo) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public Object correlationId() throws ClientException {
        if (properties() != null) {
            return properties().getCorrelationId();
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage correlationId(Object correlationId) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public String contentType() throws ClientException {
        if (properties() != null) {
            return properties().getContentType();
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage contentType(String contentType) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public String contentEncoding() throws ClientException {
        if (properties() != null) {
            return properties().getContentEncoding();
        } else {
            return null;
        }
    }

    @Override
    public Message<?> contentEncoding(String contentEncoding) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public long absoluteExpiryTime() throws ClientException {
        if (properties() != null) {
            return properties().getAbsoluteExpiryTime();
        } else {
            return 0l;
        }
    }

    @Override
    public StreamReceiverMessage absoluteExpiryTime(long expiryTime) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public long creationTime() throws ClientException {
        if (properties() != null) {
            return properties().getCreationTime();
        } else {
            return 0l;
        }
    }

    @Override
    public StreamReceiverMessage creationTime(long createTime) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public String groupId() throws ClientException {
        if (properties() != null) {
            return properties().getGroupId();
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage groupId(String groupId) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public int groupSequence() throws ClientException {
        if (properties() != null) {
            return (int) properties().getGroupSequence();
        } else {
            return 0;
        }
    }

    @Override
    public StreamReceiverMessage groupSequence(int groupSequence) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public String replyToGroupId() throws ClientException {
        if (properties() != null) {
            return properties().getReplyToGroupId();
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage replyToGroupId(String replyToGroupId) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public Properties properties() throws ClientException {
        ensureStreamDecodedTo(StreamState.PROPERTIES_READ);
        return properties;
    }

    @Override
    public StreamReceiverMessage properties(Properties properties) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Delivery Annotations API (Internal Access Only)

    DeliveryAnnotations deliveryAnnotations() throws ClientException {
        ensureStreamDecodedTo(StreamState.DELIVERY_ANNOTATIONS_READ);
        return deliveryAnnotations;
    }

    //----- Message Annotations API

    @Override
    public Object annotation(String key) throws ClientException {
        if (hasAnnotations()) {
            return annotations.getValue().get(Symbol.valueOf(key));
        } else {
            return null;
        }
    }

    @Override
    public boolean hasAnnotation(String key) throws ClientException {
        if (hasAnnotations()) {
            return annotations.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    @Override
    public boolean hasAnnotations() throws ClientException {
        ensureStreamDecodedTo(StreamState.MESSAGE_ANNOTATIONS_READ);
        return annotations != null && annotations.getValue() != null && annotations.getValue().size() > 0;
    }

    @Override
    public Object removeAnnotation(String key) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public StreamReceiverMessage forEachAnnotation(BiConsumer<String, Object> action) throws ClientException {
        if (hasAnnotations()) {
            annotations.getValue().forEach((key, value) -> {
                action.accept(key.toString(), value);
            });
        }

        return this;
    }

    @Override
    public StreamReceiverMessage annotation(String key, Object value) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public MessageAnnotations annotations() throws ClientException {
        if (hasAnnotations()) {
            return annotations;
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage annotations(MessageAnnotations messageAnnotations) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Application Properties API

    @Override
    public Object applicationProperty(String key) throws ClientException {
        if (hasApplicationProperties()) {
            return applicationProperties.getValue().get(key);
        } else {
            return null;
        }
    }

    @Override
    public boolean hasApplicationProperty(String key) throws ClientException {
        if (hasApplicationProperties()) {
            return applicationProperties.getValue().containsKey(key);
        } else {
            return false;
        }
    }

    @Override
    public boolean hasApplicationProperties() throws ClientException {
        ensureStreamDecodedTo(StreamState.APPLICATION_PROPERTIES_READ);
        return applicationProperties != null &&
               applicationProperties.getValue() != null &&
               applicationProperties.getValue().size() > 0;
    }

    @Override
    public Object removeApplicationProperty(String key) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public StreamReceiverMessage forEachApplicationProperty(BiConsumer<String, Object> action) throws ClientException {
        if (hasApplicationProperties()) {
            applicationProperties.getValue().forEach(action);
        }
        return this;
    }

    @Override
    public StreamReceiverMessage applicationProperty(String key, Object value) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public ApplicationProperties applicationProperties() throws ClientException {
        if (hasApplicationProperties()) {
            return applicationProperties;
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage applicationProperties(ApplicationProperties applicationProperties) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Message Footer API

    @Override
    public Object footer(String key) throws ClientException {
        if (hasFooters()) {
            return footer.getValue().get(Symbol.valueOf(key));
        } else {
            return null;
        }
    }

    @Override
    public boolean hasFooter(String key) throws ClientException {
        if (hasFooters()) {
            return footer.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    @Override
    public boolean hasFooters() throws ClientException {
        ensureStreamDecodedTo(StreamState.FOOTER_READ);
        return footer != null && footer.getValue() != null && footer.getValue().size() > 0;
    }

    @Override
    public Object removeFooter(String key) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public StreamReceiverMessage forEachFooter(BiConsumer<String, Object> action) throws ClientException {
        if (hasFooters()) {
            footer.getValue().forEach((key, value) -> {
                action.accept(key.toString(), value);
            });
        }

        return this;
    }

    @Override
    public StreamReceiverMessage footer(String key, Object value) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public Footer footer() throws ClientException {
        if (hasFooters()) {
            return footer;
        } else {
            return null;
        }
    }

    @Override
    public StreamReceiverMessage footer(Footer footer) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Message Body Access API

    @Override
    public StreamReceiverMessage addBodySection(Section<?> bodySection) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot encode from an StreamReceiverMessage instance.");
    }

    @Override
    public StreamReceiverMessage bodySections(Collection<Section<?>> sections) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot encode from an StreamReceiverMessage instance.");
    }

    @Override
    public Collection<Section<?>> bodySections() throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot decode all body sections from a StreamReceiverMessage instance.");
    }

    @Override
    public StreamReceiverMessage forEachBodySection(Consumer<Section<?>> consumer) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot decode all body sections from a StreamReceiverMessage instance.");
    }

    @Override
    public StreamReceiverMessage clearBodySections() throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot encode from an StreamReceiverMessage instance.");
    }

    @Override
    public InputStream body() throws ClientException {
        if (currentState.ordinal() > StreamState.BODY_READ.ordinal()) {
            throw new ClientIllegalStateException("Cannot read body from message whose body has already been read.");
        }

        ensureStreamDecodedTo(StreamState.BODY_READABLE);

        if (bodyStream == null) {
            bodyStream = new MessageBodyInputStream(deliveryStream, bodySize);
        }

        return bodyStream;
    }

    @Override
    public StreamReceiverMessage body(InputStream value) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot encode from an StreamReceiverMessage instance.");
    }

    //----- AdvancedMessage encoding API implementation.

    @Override
    public ProtonBuffer encode(Map<String, Object> deliveryAnnotations) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot encode from an StreamReceiverMessage instance.");
    }

    //----- Internal Streamed Delivery API and support methods

    private void checkClosedOrAborted() throws ClientIllegalStateException {
        if (receiver.isClosed()) {
            throw new ClientIllegalStateException("The parent Receiver instance has already been closed.");
        }

        if (aborted()) {
            throw new ClientIllegalStateException("The incoming delivery was aborted.");
        }
    }

    private void ensureStreamDecodedTo(StreamState desiredState) throws ClientException {
        checkClosedOrAborted();

        if (currentState.ordinal() < desiredState.ordinal()) {
            while (currentState.ordinal() < desiredState.ordinal()) {
                final StreamTypeDecoder<?> decoder;
                try {
                    decoder = protonDecoder.peekNextTypeDecoder(deliveryStream, decoderState);
                } catch (DecodeException dex) {
                    // TODO: Handle EOF on delivery stream reading and move to last state
                    // currentState = StreamState.FOOTER_READ;
                    // TODO: Handle inability to decode stream chunk
                    throw new ClientException("Failed reading incoming message data");
                }

                final Class<?> typeClass = decoder.getTypeClass();

                if (typeClass == Header.class) {
                    currentState = StreamState.HEADER_READ;
                } else if (typeClass == DeliveryAnnotations.class) {
                    currentState = StreamState.DELIVERY_ANNOTATIONS_READ;
                } else if (typeClass == MessageAnnotations.class) {
                    currentState = StreamState.MESSAGE_ANNOTATIONS_READ;
                } else if (typeClass == Properties.class) {
                    currentState = StreamState.PROPERTIES_READ;
                } else if (typeClass == ApplicationProperties.class) {
                    currentState = StreamState.APPLICATION_PROPERTIES_READ;
                } else if (typeClass == AmqpValue.class) {
                    currentState = StreamState.FOOTER_READ;
                } else if (typeClass == AmqpSequence.class ||
                           typeClass == Data.class ||
                           typeClass == AmqpValue.class) {
                    currentState = StreamState.BODY_READABLE;
                    bodySize = 0;  // TODO: Peek ahead to size of first body Section
                } else {
                    break; // TODO: Unknown or unexpected section in message
                }
            }
        }
    }

    //----- Internal InputStream implementations

    private class MessageBodyInputStream extends FilterInputStream {

        private final long bodySize;
        private long bytesRead;
        private boolean closed;

        public MessageBodyInputStream(InputStream deliveryStream, long bodySize) {
            super(deliveryStream);

            if (bodySize < 0) {
                throw new IllegalArgumentException("Cannot read from encoded body with size encoded as less < 0");
            }

            this.bodySize = bodySize;
        }

        @Override
        public void close() throws IOException {
            // TODO: Handle close before payload fully read.
            this.closed = true;
            super.close();
        }

        @Override
        public int read() throws IOException {
            checkClosed();

            while (true) {
                if (bytesRead >= bodySize) {
                    try {
                        ensureStreamDecodedTo(StreamState.BODY_READABLE);
                    } catch (ClientException e) {
                        throw new IOException(e);
                    }

                    if (currentState == StreamState.FOOTER_READ) {
                        return -1;  // Cannot read any further.
                    }
                }

                bytesRead++;

                return super.read();
            }
        }

        @Override
        public int read(byte target[], int offset, int length) throws IOException {
            checkClosed();

            // TODO: Read requested handling crossover to next section if possible

            return super.read(target, offset, length);
        }

        @Override
        public long skip(long amount) throws IOException {
            checkClosed();

            // TODO: Skip requested handling crossover to next section if possible

            return super.skip(amount);
        }

        private void checkClosed() throws IOException {
            if (closed) {
                throw new IOException("Stream was closed previously");
            }
        }
    }
}
