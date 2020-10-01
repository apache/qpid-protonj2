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
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.StreamDelivery;
import org.apache.qpid.protonj2.client.StreamReceiver;
import org.apache.qpid.protonj2.client.StreamReceiverMessage;
import org.apache.qpid.protonj2.client.exceptions.ClientIllegalStateException;
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

    private final ClientStreamReceiver receiver;
    private final IncomingDelivery protonDelivery;

    private DeliveryAnnotations deliveryAnnotations;

    ClientStreamReceiverMessage(ClientStreamReceiver receiver, IncomingDelivery delivery) {
        this.receiver = receiver;
        this.protonDelivery = delivery.setLinkedResource(this);
    }

    @Override
    public StreamReceiver receiver() {
        return receiver;
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

    //----- Internal Streamed Delivery API and support methods

    IncomingDelivery protonDelivery() {
        return protonDelivery;
    }

    void deliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
        this.deliveryAnnotations = deliveryAnnotations;
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

    @Override
    public Header header() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> header(Header header) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MessageAnnotations annotations() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> annotations(MessageAnnotations messageAnnotations) {
        // TODO Auto-generated method stub
        return null;
    }

    DeliveryAnnotations deliveryAnnotations() {
        return deliveryAnnotations;
    }

    @Override
    public Properties properties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> properties(Properties properties) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ApplicationProperties applicationProperties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> applicationProperties(ApplicationProperties applicationProperties) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Footer footer() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> footer(Footer footer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> messageFormat(int messageFormat) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> addBodySection(Section<?> bodySection) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> bodySections(Collection<Section<?>> sections) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<Section<?>> bodySections() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> forEachBodySection(Consumer<Section<?>> consumer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AdvancedMessage<InputStream> clearBodySections() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ProtonBuffer encode(Map<String, Object> deliveryAnnotations) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean durable() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Message<InputStream> durable(boolean durable) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte priority() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<InputStream> priority(byte priority) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long timeToLive() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<InputStream> timeToLive(long timeToLive) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean firstAcquirer() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Message<InputStream> firstAcquirer(boolean firstAcquirer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long deliveryCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<InputStream> deliveryCount(long deliveryCount) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object messageId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> messageId(Object messageId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] userId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> userId(byte[] userId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String to() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> to(String to) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String subject() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> subject(String subject) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String replyTo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> replyTo(String replyTo) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object correlationId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> correlationId(Object correlationId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String contentType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> contentType(String contentType) {
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
    public Message<InputStream> absoluteExpiryTime(long expiryTime) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long creationTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<InputStream> creationTime(long createTime) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String groupId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> groupId(String groupId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int groupSequence() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Message<InputStream> groupSequence(int groupSequence) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String replyToGroupId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> replyToGroupId(String replyToGroupId) {
        // TODO Auto-generated method stub
        return null;
    }

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
    public Object removeAnnotation(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> forEachAnnotation(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> annotation(String key, Object value) {
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
    public Message<InputStream> forEachApplicationProperty(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> applicationProperty(String key, Object value) {
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
    public Message<InputStream> forEachFooter(BiConsumer<String, Object> action) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> footer(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public InputStream body() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Message<InputStream> body(InputStream value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public StreamDelivery delivery() {
        // TODO Auto-generated method stub
        return null;
    }
}
