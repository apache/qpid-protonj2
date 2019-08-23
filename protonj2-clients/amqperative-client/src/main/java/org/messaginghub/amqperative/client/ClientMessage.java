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
package org.messaginghub.amqperative.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.function.Supplier;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Footer;
import org.apache.qpid.proton4j.amqp.messaging.Header;
import org.apache.qpid.proton4j.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Properties;
import org.apache.qpid.proton4j.amqp.messaging.Section;
import org.messaginghub.amqperative.Message;

public class ClientMessage<E> implements Message<E> {

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Footer footer;

    private final Supplier<Section> sectionSupplier;
    private E body;

    /**
     * Create a new {@link ClientMessage} instance with a {@link Supplier} that will
     * provide the AMQP {@link Section} value for any body that is set on the message.
     *
     * @param sectionSupplier
     *      A {@link Supplier} that will generate Section values for the message body.
     */
    ClientMessage(Supplier<Section> sectionSupplier) {
        this.sectionSupplier = sectionSupplier;
    }

    //----- Entry point for creating new ClientMessage instances.

    public static <V> Message<V> create(V body, Supplier<Section> sectionSupplier) {
        return new ClientMessage<V>(sectionSupplier).setBody(body);
    }

    //----- Message Header API

    @Override
    public boolean isDurable() {
        return header == null ? Header.DEFAULT_DURABILITY : header.isDurable();
    }

    @Override
    public ClientMessage<E> setDurable(boolean durable) {
        lazyCreateHeader().setDurable(durable);
        return this;
    }

    @Override
    public byte getPriority() {
        return header != null ? Header.DEFAULT_PRIORITY : header.getPriority();
    }

    @Override
    public ClientMessage<E> setPriority(byte priority) {
        lazyCreateHeader().setPriority(priority);
        return this;
    }

    @Override
    public long getTimeToLive() {
        return header != null ? Header.DEFAULT_TIME_TO_LIVE : header.getPriority();
    }

    @Override
    public ClientMessage<E> setTimeToLive(long timeToLive) {
        lazyCreateHeader().setTimeToLive(timeToLive);
        return this;
    }

    @Override
    public boolean getFirstAcquirer() {
        return header != null ? Header.DEFAULT_FIRST_ACQUIRER : header.isFirstAcquirer();
    }

    @Override
    public ClientMessage<E> setFirstAcquirer(boolean firstAcquirer) {
        lazyCreateHeader().setFirstAcquirer(firstAcquirer);
        return this;
    }

    @Override
    public long getDeliveryCount() {
        return header != null ? Header.DEFAULT_DELIVERY_COUNT : header.getDeliveryCount();
    }

    @Override
    public ClientMessage<E> setDeliveryCount(long deliveryCount) {
        lazyCreateHeader().setDeliveryCount(deliveryCount);
        return this;
    }

    //----- Message Properties access

    @Override
    public Object getMessageId() {
        return properties != null ? properties.getMessageId() : null;
    }

    @Override
    public Message<?> setMessageId(Object messageId) {
        lazyCreateProperties().setMessageId(messageId);
        return this;
    }

    @Override
    public byte[] getUserId() {
        byte[] copyOfUserId = null;
        if (properties != null && properties.getUserId() != null) {
            copyOfUserId = properties.getUserId().arrayCopy();
        }

        return copyOfUserId;
    }

    @Override
    public Message<?> setUserId(byte[] userId) {
        lazyCreateProperties().setUserId(new Binary(Arrays.copyOf(userId, userId.length)));
        return this;
    }

    @Override
    public String getTo() {
        return properties != null ? properties.getTo() : null;
    }

    @Override
    public Message<?> setTo(String to) {
        lazyCreateProperties().setTo(to);
        return this;
    }

    @Override
    public String getSubject() {
        return properties != null ? properties.getSubject() : null;
    }

    @Override
    public Message<?> setSubject(String subject) {
        lazyCreateProperties().setSubject(subject);
        return this;
    }

    @Override
    public String getReplyTo() {
        return properties != null ? properties.getReplyTo() : null;
    }

    @Override
    public Message<?> setReplyTo(String replyTo) {
        lazyCreateProperties().setReplyTo(replyTo);
        return this;
    }

    @Override
    public Object getCorrelationId() {
        return properties != null ? properties.getCorrelationId() : null;
    }

    @Override
    public Message<?> setCorrelationId(Object correlationId) {
        lazyCreateProperties().setCorrelationId(correlationId);
        return this;
    }

    @Override
    public String getContentType() {
        return properties != null ? properties.getContentType() : null;
    }

    @Override
    public Message<?> setContentType(String contentType) {
        lazyCreateProperties().setContentType(contentType);
        return this;
    }

    @Override
    public String getContentEncoding() {
        return properties != null ? properties.getContentEncoding() : null;
    }

    @Override
    public Message<?> setContentEncoding(String contentEncoding) {
        lazyCreateProperties().setContentEncoding(contentEncoding);
        return this;
    }

    @Override
    public long getAbsoluteExpiryTime() {
        return properties != null ? properties.getAbsoluteExpiryTime() : null;
    }

    @Override
    public Message<E> setAbsoluteExpiryTime(long expiryTime) {
        lazyCreateProperties().setAbsoluteExpiryTime(expiryTime);
        return this;
    }

    @Override
    public long getCreationTime() {
        return properties != null ? properties.getCreationTime() : null;
    }

    @Override
    public Message<E> setCreationTime(long createTime) {
        lazyCreateProperties().setCreationTime(createTime);
        return this;
    }

    @Override
    public String getGroupId() {
        return properties != null ? properties.getGroupId() : null;
    }

    @Override
    public Message<E> setGroupId(String groupId) {
        lazyCreateProperties().setGroupId(groupId);
        return this;
    }

    @Override
    public int getGroupSequence() {
        return properties != null ? (int) properties.getGroupSequence() : null;
    }

    @Override
    public Message<E> setGroupSequence(int groupSequence) {
        lazyCreateProperties().setGroupSequence(groupSequence);
        return this;
    }

    @Override
    public String getReplyToGroupId() {
        return properties != null ? properties.getReplyToGroupId() : null;
    }

    @Override
    public Message<E> setReplyToGroupId(String replyToGroupId) {
        lazyCreateProperties().setReplyToGroupId(replyToGroupId);
        return this;
    }

    //----- Delivery Annotations Access

    public Object getDeliveryAnnotations(String key) {
        Object value = null;
        if (deliveryAnnotations != null) {
            value = deliveryAnnotations.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    public ClientMessage<E> setDeliveryAnnotation(String key, Object value) {
        lazyCreateDeliveryAnnotations().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Message Annotations Access

    public Object getMessageAnnotation(String key) {
        Object value = null;
        if (messageAnnotations != null) {
            value = messageAnnotations.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    public ClientMessage<E> setMessageAnnotation(String key, Object value) {
        lazyCreateMessageAnnotations().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Application Properties Access

    public Object getApplicationProperty(String key) {
        Object value = null;
        if (applicationProperties != null) {
            value = applicationProperties.getValue().get(key);
        }

        return value;
    }

    public ClientMessage<E> setApplicationProperty(String key, Object value) {
        lazyCreateApplicationProperties().getValue().put(key,value);
        return this;
    }

    //----- Footer Access

    public Object getFooter(String key) {
        Object value = null;
        if (footer != null) {
            value = footer.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    public ClientMessage<E> setFooter(String key, Object value) {
        lazyCreateFooter().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Message body access

    @Override
    public E getBody() {
        return body;
    }

    ClientMessage<E> setBody(E body) {
        this.body = body;
        return this;
    }

    //----- Access to proton resources

    Header getHeader() {
        return header;
    }

    Message<?> setHeader(Header header) {
        this.header = header;
        return this;
    }

    DeliveryAnnotations getDeliveryAnnotations() {
        return deliveryAnnotations;
    }

    Message<E> setDeliveryAnnotations(DeliveryAnnotations annotations) {
        this.deliveryAnnotations = annotations;
        return this;
    }

    MessageAnnotations getMessageAnnotations() {
        return messageAnnotations;
    }

    Message<?> setMessageAnnotations(MessageAnnotations annotations) {
        this.messageAnnotations = annotations;
        return this;
    }

    Properties getProperties() {
        return properties;
    }

    Message<?> setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    ApplicationProperties getApplicationProperties() {
        return applicationProperties;
    }

    Message<?> setApplicationProperties(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
        return this;
    }

    Footer getFooter() {
        return footer;
    }

    Message<?> setFooter(Footer footer) {
        this.footer = footer;
        return this;
    }

    Section getBodySection() {
        return sectionSupplier.get();
    }

    //----- Internal API

    private Header lazyCreateHeader() {
        if (header == null) {
            header = new Header();
        }

        return header;
    }

    private Properties lazyCreateProperties() {
        if (properties == null) {
            properties = new Properties();
        }

        return properties;
    }

    private ApplicationProperties lazyCreateApplicationProperties() {
        if (applicationProperties == null) {
            applicationProperties = new ApplicationProperties(new HashMap<>());
        }

        return applicationProperties;
    }

    private MessageAnnotations lazyCreateMessageAnnotations() {
        if (messageAnnotations == null) {
            messageAnnotations = new MessageAnnotations(new HashMap<>());
        }

        return messageAnnotations;
    }

    private DeliveryAnnotations lazyCreateDeliveryAnnotations() {
        if (deliveryAnnotations == null) {
            deliveryAnnotations = new DeliveryAnnotations(new HashMap<>());
        }

        return deliveryAnnotations;
    }

    private Footer lazyCreateFooter() {
        if (footer == null) {
            footer = new Footer(new HashMap<>());
        }

        return footer;
    }
}
