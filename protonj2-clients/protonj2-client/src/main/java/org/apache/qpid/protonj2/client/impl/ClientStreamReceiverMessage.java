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

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonCompositeBuffer;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.StreamReceiverMessage;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
import org.apache.qpid.protonj2.client.exceptions.ClientUnsupportedOperationException;
import org.apache.qpid.protonj2.engine.IncomingDelivery;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
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
        PREAMBLE_READ,
        BODY_READING,
        BODY_READ,
        FOOTER_READ
    }

    private final ClientStreamReceiver receiver;
    private final ClientStreamDelivery delivery;
    private final IncomingDelivery protonDelivery;
    private final ProtonCompositeBuffer deliveryBuffer = new ProtonCompositeBuffer();

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations annotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Footer footer;

    ClientStreamReceiverMessage(ClientStreamReceiver receiver, ClientStreamDelivery delivery) {
        this.receiver = receiver;
        this.delivery = delivery;
        this.protonDelivery = delivery.getProtonDelivery();
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
    public int messageFormat() {
        return protonDelivery != null ? protonDelivery.getMessageFormat() : 0;
    }

    @Override
    public StreamReceiverMessage messageFormat(int messageFormat) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiverMessage");
    }

    //----- Header API implementation

    @Override
    public boolean durable() {
        return header() != null ? header.isDurable() : false;
    }

    @Override
    public StreamReceiverMessage durable(boolean durable) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public byte priority() {
        return header() != null ? header.getPriority() : Header.DEFAULT_PRIORITY;
    }

    @Override
    public StreamReceiverMessage priority(byte priority) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public long timeToLive() {
        return header() != null ? header.getTimeToLive() : Header.DEFAULT_TIME_TO_LIVE;
    }

    @Override
    public StreamReceiverMessage timeToLive(long timeToLive) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public boolean firstAcquirer() {
        return header() != null ? header.isFirstAcquirer() : Header.DEFAULT_FIRST_ACQUIRER;
    }

    @Override
    public StreamReceiverMessage firstAcquirer(boolean firstAcquirer) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public long deliveryCount() {
        return header() != null ? header.getDeliveryCount() : Header.DEFAULT_DELIVERY_COUNT;
    }

    @Override
    public StreamReceiverMessage deliveryCount(long deliveryCount) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public Header header() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamReceiverMessage header(Header header) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Properties API implementation

    @Override
    public Object messageId() {
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
    public byte[] userId() {
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
    public String to() {
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
    public String subject() {
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
    public String replyTo() {
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
    public Object correlationId() {
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
    public String contentType() {
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
    public String contentEncoding() {
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
    public long absoluteExpiryTime() {
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
    public long creationTime() {
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
    public String groupId() {
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
    public int groupSequence() {
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
    public String replyToGroupId() {
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
    public Properties properties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamReceiverMessage properties(Properties properties) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Delivery Annotations API (Internal Access Only)

    DeliveryAnnotations deliveryAnnotations() throws ClientException {
        return null;
    }

    //----- Message Annotations API

    @Override
    public Object annotation(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean hasAnnotation(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasAnnotations() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Object removeAnnotation(String key) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public StreamReceiverMessage forEachAnnotation(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public StreamReceiverMessage annotation(String key, Object value) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public MessageAnnotations annotations() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamReceiverMessage annotations(MessageAnnotations messageAnnotations) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Application Properties API

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
    public Object removeApplicationProperty(String key) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public StreamReceiverMessage forEachApplicationProperty(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public StreamReceiverMessage applicationProperty(String key, Object value) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public ApplicationProperties applicationProperties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamReceiverMessage applicationProperties(ApplicationProperties applicationProperties) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    //----- Message Footer API

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
    public Object removeFooter(String key) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public StreamReceiverMessage forEachFooter(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public StreamReceiverMessage footer(String key, Object value) throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot write to a StreamReceiveMessage");
    }

    @Override
    public Footer footer() {
        // TODO Auto-generated method stub
        return null;
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
    public Collection<Section<?>> bodySections() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamReceiverMessage forEachBodySection(Consumer<Section<?>> consumer) {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public StreamReceiverMessage clearBodySections() throws ClientUnsupportedOperationException {
        throw new ClientUnsupportedOperationException("Cannot encode from an StreamReceiverMessage instance.");
    }

    @Override
    public InputStream body() {
        // TODO Auto-generated method stub
        return null;
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

    //----- Event Handlers for Delivery updates

    void handleDeliveryRead(IncomingDelivery delivery) {
        // TODO: break any waiting for read cases
    }

    void handleDeliveryAborted(IncomingDelivery delivery) {
        // TODO: break any waiting for read cases
    }

    void handleReceiverClosed(ClientStreamReceiver receiver) {
        // TODO: break any waiting for read cases
    }

    //----- Internal InputStream implementations

}
