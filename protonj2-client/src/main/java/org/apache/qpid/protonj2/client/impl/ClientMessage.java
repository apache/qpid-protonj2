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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.apache.qpid.protonj2.types.messaging.Section.SectionType;

/**
 * Client provided {@link AdvancedMessage} implementation that is used when sending messages
 * from a {@link ClientSender} or when decoding an AMQP Transfer for which all frames have
 * arrived.
 *
 * @param <E> the body type that the {@link Message} carries
 */
public class ClientMessage<E> implements AdvancedMessage<E> {

    private Header header;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Section<E> body;
    private List<Section<?>> bodySections;
    private Footer footer;

    private int messageFormat;

    /**
     * Create a new {@link ClientMessage} instance with no default body section or
     * section supplier
     *
     * @param sectionSupplier
     *      A {@link Supplier} that will generate Section values for the message body.
     */
    ClientMessage() {
        this.body = null;
    }

    /**
     * Create a new {@link ClientMessage} instance with a {@link Supplier} that will
     * provide the AMQP {@link Section} value for any body that is set on the message.
     *
     * @param body
     *      The object that comprises the value portion of the body {@link Section}.
     */
    ClientMessage(Section<E> body) {
        this.body = body;
    }

    @Override
    public AdvancedMessage<E> toAdvancedMessage() {
        return this;
    }

    //----- Entry point for creating new ClientMessage instances.

    /**
     * Creates an empty {@link ClientMessage} instance.
     *
     * @param <V> The type of the body value carried in this message.
     *
     * @return a new empty {@link ClientMessage} instance.
     */
    public static <V> ClientMessage<V> create() {
        return new ClientMessage<V>();
    }

    /**
     * Creates an {@link ClientMessage} instance with the given body {@link Section} value.
     *
     * @param <V> The type of the body value carried in this message body section.
     *
     * @param body
     *      The body {@link Section} to assign to the created message isntance.
     *
     * @return a new {@link ClientMessage} instance with the given body.
     */
    public static <V> ClientMessage<V> create(Section<V> body) {
        return new ClientMessage<V>(body);
    }

    /**
     * Creates an empty {@link ClientMessage} instance.
     *
     * @param <V> The type of the body value carried in this message.
     *
     * @return a new empty {@link ClientMessage} instance.
     */
    public static <V> ClientMessage<V> createAdvancedMessage() {
        return new ClientMessage<V>();
    }

    //----- Message Header API

    @Override
    public boolean durable() {
        return header == null ? Header.DEFAULT_DURABILITY : header.isDurable();
    }

    @Override
    public ClientMessage<E> durable(boolean durable) {
        lazyCreateHeader().setDurable(durable);
        return this;
    }

    @Override
    public byte priority() {
        return header == null ? Header.DEFAULT_PRIORITY : header.getPriority();
    }

    @Override
    public ClientMessage<E> priority(byte priority) {
        lazyCreateHeader().setPriority(priority);
        return this;
    }

    @Override
    public long timeToLive() {
        return header == null ? Header.DEFAULT_TIME_TO_LIVE : header.getTimeToLive();
    }

    @Override
    public ClientMessage<E> timeToLive(long timeToLive) {
        lazyCreateHeader().setTimeToLive(timeToLive);
        return this;
    }

    @Override
    public boolean firstAcquirer() {
        return header == null ? Header.DEFAULT_FIRST_ACQUIRER : header.isFirstAcquirer();
    }

    @Override
    public ClientMessage<E> firstAcquirer(boolean firstAcquirer) {
        lazyCreateHeader().setFirstAcquirer(firstAcquirer);
        return this;
    }

    @Override
    public long deliveryCount() {
        return header == null ? Header.DEFAULT_DELIVERY_COUNT : header.getDeliveryCount();
    }

    @Override
    public ClientMessage<E> deliveryCount(long deliveryCount) {
        lazyCreateHeader().setDeliveryCount(deliveryCount);
        return this;
    }

    //----- Message Properties access

    @Override
    public Object messageId() {
        return properties != null ? properties.getMessageId() : null;
    }

    @Override
    public Message<E> messageId(Object messageId) {
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
    public Message<E> userId(byte[] userId) {
        lazyCreateProperties().setUserId(new Binary(Arrays.copyOf(userId, userId.length)));
        return this;
    }

    @Override
    public String to() {
        return properties != null ? properties.getTo() : null;
    }

    @Override
    public Message<E> to(String to) {
        lazyCreateProperties().setTo(to);
        return this;
    }

    @Override
    public String subject() {
        return properties != null ? properties.getSubject() : null;
    }

    @Override
    public Message<E> subject(String subject) {
        lazyCreateProperties().setSubject(subject);
        return this;
    }

    @Override
    public String replyTo() {
        return properties != null ? properties.getReplyTo() : null;
    }

    @Override
    public Message<E> replyTo(String replyTo) {
        lazyCreateProperties().setReplyTo(replyTo);
        return this;
    }

    @Override
    public Object correlationId() {
        return properties != null ? properties.getCorrelationId() : null;
    }

    @Override
    public Message<E> correlationId(Object correlationId) {
        lazyCreateProperties().setCorrelationId(correlationId);
        return this;
    }

    @Override
    public String contentType() {
        return properties != null ? properties.getContentType() : null;
    }

    @Override
    public Message<E> contentType(String contentType) {
        lazyCreateProperties().setContentType(contentType);
        return this;
    }

    @Override
    public String contentEncoding() {
        return properties != null ? properties.getContentEncoding() : null;
    }

    @Override
    public Message<E> contentEncoding(String contentEncoding) {
        lazyCreateProperties().setContentEncoding(contentEncoding);
        return this;
    }

    @Override
    public long absoluteExpiryTime() {
        return properties != null ? properties.getAbsoluteExpiryTime() : 0;
    }

    @Override
    public Message<E> absoluteExpiryTime(long expiryTime) {
        lazyCreateProperties().setAbsoluteExpiryTime(expiryTime);
        return this;
    }

    @Override
    public long creationTime() {
        return properties != null ? properties.getCreationTime() : 0;
    }

    @Override
    public Message<E> creationTime(long createTime) {
        lazyCreateProperties().setCreationTime(createTime);
        return this;
    }

    @Override
    public String groupId() {
        return properties != null ? properties.getGroupId() : null;
    }

    @Override
    public Message<E> groupId(String groupId) {
        lazyCreateProperties().setGroupId(groupId);
        return this;
    }

    @Override
    public int groupSequence() {
        return properties != null ? (int) properties.getGroupSequence() : 0;
    }

    @Override
    public Message<E> groupSequence(int groupSequence) {
        lazyCreateProperties().setGroupSequence(groupSequence);
        return this;
    }

    @Override
    public String replyToGroupId() {
        return properties != null ? properties.getReplyToGroupId() : null;
    }

    @Override
    public Message<E> replyToGroupId(String replyToGroupId) {
        lazyCreateProperties().setReplyToGroupId(replyToGroupId);
        return this;
    }

    //----- Message Annotations Access

    @Override
    public Object annotation(String key) {
        if (hasAnnotations()) {
            return messageAnnotations.getValue().get(Symbol.valueOf(key));
        } else {
            return null;
        }
    }

    @Override
    public boolean hasAnnotation(String key) {
        if (hasAnnotations()) {
            return messageAnnotations.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    @Override
    public boolean hasAnnotations() {
        return messageAnnotations != null &&
               messageAnnotations.getValue() != null &&
               messageAnnotations.getValue().size() > 0;
    }

    @Override
    public Object removeAnnotation(String key) {
        if (hasAnnotations()) {
            return messageAnnotations.getValue().remove(Symbol.valueOf(key));
        } else {
            return null;
        }
     }

    @Override
    public Message<E> forEachAnnotation(BiConsumer<String, Object> action) {
        if (hasAnnotations()) {
            messageAnnotations.getValue().forEach((key, value) -> {
                action.accept(key.toString(), value);
            });
        }

        return this;
    }

    @Override
    public ClientMessage<E> annotation(String key, Object value) {
        lazyCreateMessageAnnotations().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Application Properties Access

    @Override
    public Object property(String key) {
        if (hasProperties()) {
            return applicationProperties.getValue().get(key);
        } else {
            return null;
        }
    }

    @Override
    public boolean hasProperty(String key) {
        if (hasProperties()) {
            return applicationProperties.getValue().containsKey(key);
        } else {
            return false;
        }
    }

    @Override
    public boolean hasProperties() {
        return applicationProperties != null &&
               applicationProperties.getValue() != null &&
               applicationProperties.getValue().size() > 0;
    }

    @Override
    public Object removeProperty(String key) {
        if (hasProperties()) {
            return applicationProperties.getValue().remove(key);
        } else {
            return null;
        }
     }

    @Override
    public Message<E> forEachProperty(BiConsumer<String, Object> action) {
        if (hasProperties()) {
            applicationProperties.getValue().forEach(action);
        }

        return this;
    }

    @Override
    public ClientMessage<E> property(String key, Object value) {
        lazyCreateApplicationProperties().getValue().put(key,value);
        return this;
    }

    //----- Footer Access

    @Override
    public Object footer(String key) {
        if (hasFooters()) {
            return footer.getValue().get(Symbol.valueOf(key));
        } else {
            return null;
        }
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
    public Message<E> forEachFooter(BiConsumer<String, Object> action) {
        if (hasFooters()) {
            footer.getValue().forEach((key, value) -> {
                action.accept(key.toString(), value);
            });
        }

        return this;
    }

    @Override
    public ClientMessage<E> footer(String key, Object value) {
        lazyCreateFooter().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Message body access

    @SuppressWarnings("unchecked")
    @Override
    public E body() {
        Section<E> section = body;

        if (bodySections != null) {
            section = (Section<E>) bodySections.get(0);
        }

        return section != null ? section.getValue() : null;
    }

    @Override
    public ClientMessage<E> body(E value) {
        if (bodySections != null) {
            if (value != null) {
                bodySections.set(0, ClientMessageSupport.createSectionFromValue(value));
            } else {
                bodySections = null;
            }
        } else {
            body = ClientMessageSupport.createSectionFromValue(value);
        }

        return this;
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
            applicationProperties = new ApplicationProperties(new LinkedHashMap<>());
        }

        return applicationProperties;
    }

    private MessageAnnotations lazyCreateMessageAnnotations() {
        if (messageAnnotations == null) {
            messageAnnotations = new MessageAnnotations(new LinkedHashMap<>());
        }

        return messageAnnotations;
    }

    private Footer lazyCreateFooter() {
        if (footer == null) {
            footer = new Footer(new LinkedHashMap<>());
        }

        return footer;
    }

    //----- AdvancedMessage interface implementation

    @Override
    public Header header() {
        return header;
    }

    @Override
    public ClientMessage<E> header(Header header) {
        this.header = header;
        return this;
    }

    @Override
    public MessageAnnotations annotations() {
        return messageAnnotations;
    }

    @Override
    public ClientMessage<E> annotations(MessageAnnotations messageAnnotations) {
        this.messageAnnotations = messageAnnotations;
        return this;
    }

    @Override
    public Properties properties() {
        return properties;
    }

    @Override
    public ClientMessage<E> properties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public ApplicationProperties applicationProperties() {
        return applicationProperties;
    }

    @Override
    public ClientMessage<E> applicationProperties(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
        return this;
    }

    @Override
    public Footer footer() {
        return footer;
    }

    @Override
    public ClientMessage<E> footer(Footer footer) {
        this.footer = footer;
        return this;
    }

    @Override
    public int messageFormat() {
        return messageFormat;
    }

    @Override
    public ClientMessage<E> messageFormat(int messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

    @Override
    public ProtonBuffer encode(Map<String, Object> deliveryAnnotations) throws ClientException {
        return ClientMessageSupport.encodeMessage(this, deliveryAnnotations);
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public ClientMessage<E> addBodySection(Section<?> bodySection) {
        Objects.requireNonNull(bodySection, "Additional Body Section cannot be null");

        if (body == null && bodySections == null) {
            body = (Section<E>) bodySection;
        } else {
            if (bodySections == null) {
                bodySections = new ArrayList<>();

                // Preserve older section from original message creation.
                if (body != null) {
                    bodySections.add(body);
                    body = null;
                }
            }

            bodySections.add(validateBodySections(messageFormat, bodySections, bodySection));
        }

        return this;
    }

    @Override
    public ClientMessage<E> bodySections(Collection<Section<?>> sections) {
        if (sections == null || sections.isEmpty()) {
            bodySections = null;
        } else {
            List<Section<?>> result = new ArrayList<>(sections.size());
            sections.forEach(section -> result.add(validateBodySections(messageFormat, result, section)));
            bodySections = result;
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<Section<?>> bodySections() {
        if (bodySections == null && body == null) {
            return Collections.EMPTY_LIST;
        } else {
            final Collection<Section<?>> result = new ArrayList<>();
            forEachBodySection(section -> result.add(section));
            return Collections.unmodifiableCollection(result);
        }
    }

    @Override
    public ClientMessage<E> forEachBodySection(Consumer<Section<?>> consumer) {
        if (bodySections != null) {
            bodySections.forEach(section -> {
                consumer.accept(section);
            });
        } else {
            if (body != null) {
                consumer.accept(body);
            }
        }

        return this;
    }


    @Override
    public ClientMessage<E> clearBodySections() {
        bodySections = null;
        body = null;

        return this;
    }

    private static Section<?> validateBodySections(int messageFormat, List<Section<?>> target, Section<?> section) {
        if (messageFormat == 0 && target != null && !target.isEmpty()) {
            switch (section.getType()) {
                case AmqpSequence:
                    if (target.get(0).getType() != SectionType.AmqpSequence) {
                        throw new IllegalArgumentException(
                            "Message Format violation: AmqpSequence expected but got type: " + section.getType());
                    }
                    break;
                case AmqpValue:
                    throw new IllegalArgumentException(
                        "Message Format violation: Only one AmqpValue section allowed");
                case Data:
                    if (target.get(0).getType() != SectionType.Data) {
                        throw new IllegalArgumentException(
                            "Message Format violation: Data Section expected but got type: " + section.getType());
                    }
                    break;
                default:
                    break;
            }
        }

        return section;
    }
}
