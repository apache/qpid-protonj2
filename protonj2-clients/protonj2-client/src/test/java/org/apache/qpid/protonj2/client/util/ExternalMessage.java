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
package org.apache.qpid.protonj2.client.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.impl.ClientMessageSupport;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

public class ExternalMessage<E> implements Message<E> {

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Footer footer;
    private Section<E> body;

    private final boolean allowAdvancedConversions;

    public ExternalMessage() {
        this(false);
    }

    public ExternalMessage(boolean allowAdvancedConversions) {
        this(null, allowAdvancedConversions);
    }

    public ExternalMessage(Section<E> section) {
        this(section, false);
    }

    public ExternalMessage(Section<E> body, boolean allowAdvancedConversions) {
        this.body = body;
        this.allowAdvancedConversions = allowAdvancedConversions;
    }

    @Override
    public AdvancedMessage<E> toAdvancedMessage() {
        if (allowAdvancedConversions) {
            return new AdvancedExternalMessage<E>(this);
        } else {
            throw new UnsupportedOperationException("Test ExternalMessage doesn't support AdvancedMessage conversions");
        }
    }

    //----- Entry point for creating new ClientMessage instances.

    public static <V> Message<V> create(Section<V> body) {
        return new ExternalMessage<V>(body);
    }

    //----- Message Header API

    @Override
    public boolean durable() {
        return header == null ? Header.DEFAULT_DURABILITY : header.isDurable();
    }

    @Override
    public ExternalMessage<E> durable(boolean durable) {
        lazyCreateHeader().setDurable(durable);
        return this;
    }

    @Override
    public byte priority() {
        return header == null ? Header.DEFAULT_PRIORITY : header.getPriority();
    }

    @Override
    public ExternalMessage<E> priority(byte priority) {
        lazyCreateHeader().setPriority(priority);
        return this;
    }

    @Override
    public long timeToLive() {
        return header == null ? Header.DEFAULT_TIME_TO_LIVE : header.getTimeToLive();
    }

    @Override
    public ExternalMessage<E> timeToLive(long timeToLive) {
        lazyCreateHeader().setTimeToLive(timeToLive);
        return this;
    }

    @Override
    public boolean firstAcquirer() {
        return header == null ? Header.DEFAULT_FIRST_ACQUIRER : header.isFirstAcquirer();
    }

    @Override
    public ExternalMessage<E> firstAcquirer(boolean firstAcquirer) {
        lazyCreateHeader().setFirstAcquirer(firstAcquirer);
        return this;
    }

    @Override
    public long deliveryCount() {
        return header == null ? Header.DEFAULT_DELIVERY_COUNT : header.getDeliveryCount();
    }

    @Override
    public ExternalMessage<E> deliveryCount(long deliveryCount) {
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
        return properties != null ? properties.getAbsoluteExpiryTime() : null;
    }

    @Override
    public Message<E> absoluteExpiryTime(long expiryTime) {
        lazyCreateProperties().setAbsoluteExpiryTime(expiryTime);
        return this;
    }

    @Override
    public long creationTime() {
        return properties != null ? properties.getCreationTime() : null;
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
        return properties != null ? (int) properties.getGroupSequence() : null;
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

    //----- Delivery Annotations Access

    @Override
    public Object deliveryAnnotation(String key) {
        Object value = null;
        if (deliveryAnnotations != null && deliveryAnnotations.getValue() != null) {
            value = deliveryAnnotations.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    @Override
    public boolean hasDeliveryAnnotation(String key) {
        if (deliveryAnnotations != null && deliveryAnnotations.getValue() != null) {
            return deliveryAnnotations.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    @Override
    public boolean hasDeliveryAnnotations() {
        return deliveryAnnotations != null &&
               deliveryAnnotations.getValue() != null &&
               deliveryAnnotations.getValue().size() > 0;
    }

    @Override
    public Object removeDeliveryAnnotation(String key) {
        if (hasDeliveryAnnotations()) {
            return deliveryAnnotations.getValue().remove(Symbol.valueOf(key));
        } else {
            return null;
        }
     }

    @Override
    public Message<E> forEachDeliveryAnnotation(BiConsumer<String, Object> action) {
        if (hasDeliveryAnnotations()) {
            deliveryAnnotations.getValue().forEach((key, value) -> {
                action.accept(key.toString(), value);
            });
        }

        return this;
    }

    @Override
    public ExternalMessage<E> deliveryAnnotation(String key, Object value) {
        lazyCreateDeliveryAnnotations().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Message Annotations Access

    @Override
    public Object messageAnnotation(String key) {
        Object value = null;
        if (messageAnnotations != null) {
            value = messageAnnotations.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    @Override
    public boolean hasMessageAnnotation(String key) {
        if (messageAnnotations != null && messageAnnotations.getValue() != null) {
            return messageAnnotations.getValue().containsKey(Symbol.valueOf(key));
        } else {
            return false;
        }
    }

    @Override
    public boolean hasMessageAnnotations() {
        return messageAnnotations != null &&
               messageAnnotations.getValue() != null &&
               messageAnnotations.getValue().size() > 0;
    }

    @Override
    public Object removeMessageAnnotation(String key) {
        if (hasMessageAnnotations()) {
            return messageAnnotations.getValue().remove(Symbol.valueOf(key));
        } else {
            return null;
        }
     }

    @Override
    public Message<E> forEachMessageAnnotation(BiConsumer<String, Object> action) {
        if (hasMessageAnnotations()) {
            messageAnnotations.getValue().forEach((key, value) -> {
                action.accept(key.toString(), value);
            });
        }

        return this;
    }

    @Override
    public ExternalMessage<E> messageAnnotation(String key, Object value) {
        lazyCreateMessageAnnotations().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Application Properties Access

    @Override
    public Object applicationProperty(String key) {
        Object value = null;
        if (applicationProperties != null) {
            value = applicationProperties.getValue().get(key);
        }

        return value;
    }

    @Override
    public boolean hasApplicationProperty(String key) {
        if (applicationProperties != null && applicationProperties.getValue() != null) {
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
    public Message<E> forEachApplicationProperty(BiConsumer<String, Object> action) {
        if (hasApplicationProperties()) {
            applicationProperties.getValue().forEach(action);
        }

        return this;
    }

    @Override
    public ExternalMessage<E> applicationProperty(String key, Object value) {
        lazyCreateApplicationProperties().getValue().put(key,value);
        return this;
    }

    //----- Footer Access

    @Override
    public Object footer(String key) {
        Object value = null;
        if (footer != null) {
            value = footer.getValue().get(Symbol.valueOf(key));
        }

        return value;
    }

    @Override
    public boolean hasFooter(String key) {
        if (footer != null && footer.getValue() != null) {
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
    public ExternalMessage<E> footer(String key, Object value) {
        lazyCreateFooter().getValue().put(Symbol.valueOf(key),value);
        return this;
    }

    //----- Message body access

    @Override
    public E body() {
        return body.getValue();
    }

    @Override
    public ExternalMessage<E> body(E body) {
        this.body = ClientMessageSupport.createSectionFromValue(body);
        return this;
    }

    public ExternalMessage<E> body(Section<E> body) {
        this.body = body;
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

    private DeliveryAnnotations lazyCreateDeliveryAnnotations() {
        if (deliveryAnnotations == null) {
            deliveryAnnotations = new DeliveryAnnotations(new LinkedHashMap<>());
        }

        return deliveryAnnotations;
    }

    private Footer lazyCreateFooter() {
        if (footer == null) {
            footer = new Footer(new LinkedHashMap<>());
        }

        return footer;
    }

    //----- Sealed AdvancedMessage implementation that wraps this type.

    private static class AdvancedExternalMessage<E> extends ExternalMessage<E> implements AdvancedMessage<E> {

        private final ExternalMessage<E> message;

        private final List<Section<E>> bodySections = new ArrayList<>();
        private int messageFormat;
        private boolean complete = true;
        private boolean aborted;

        /**
         * Create a wrapper that exposes {@link ExternalMessage} as an {@link AdvancedMessage}
         *
         * @param message
         *      this message to wrap.
         */
        public AdvancedExternalMessage(ExternalMessage<E> message) {
            this.message = message;
        }

        @Override
        public Header header() {
            return message.header;
        }

        @Override
        public AdvancedMessage<E> header(Header header) {
            message.header = header;
            return this;
        }

        @Override
        public DeliveryAnnotations deliveryAnnotations() {
            return message.deliveryAnnotations;
        }

        @Override
        public AdvancedMessage<E> deliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
            message.deliveryAnnotations = deliveryAnnotations;
            return this;
        }

        @Override
        public MessageAnnotations messageAnnotations() {
            return message.messageAnnotations;
        }

        @Override
        public AdvancedMessage<E> messageAnnotations(MessageAnnotations messageAnnotations) {
            message.messageAnnotations = messageAnnotations;
            return this;
        }

        @Override
        public Properties properties() {
            return message.properties;
        }

        @Override
        public AdvancedMessage<E> properties(Properties properties) {
            message.properties = properties;
            return this;
        }

        @Override
        public ApplicationProperties applicationProperties() {
            return message.applicationProperties;
        }

        @Override
        public AdvancedMessage<E> applicationProperties(ApplicationProperties applicationProperties) {
            message.applicationProperties = applicationProperties;
            return this;
        }

        @Override
        public Footer footer() {
            return message.footer;
        }

        @Override
        public AdvancedMessage<E> footer(Footer footer) {
            message.footer = footer;
            return this;
        }

        @Override
        public int messageFormat() {
            return messageFormat;
        }

        @Override
        public AdvancedMessage<E> messageFormat(int messageFormat) {
            this.messageFormat = messageFormat;
            return this;
        }

        @Override
        public ProtonBuffer encode() {
            return ClientMessageSupport.encodeMessage(this);
        }

        @Override
        public AdvancedMessage<E> addBodySection(Section<E> bodySection) {
            Objects.requireNonNull(bodySection, "Additional Body Section cannot be null");

            if (bodySections.isEmpty()) {
                // Preserve older section from original message creation.
                if (message.body() != null) {
                    bodySections.add(message.body);
                }
            }

            return this;
        }

        @Override
        public AdvancedMessage<E> bodySections(Collection<Section<E>> sections) {
            this.bodySections.clear();
            this.bodySections.addAll(sections);
            return this;
        }

        @Override
        public Collection<Section<E>> bodySections() {
            return Collections.unmodifiableCollection(bodySections);
        }

        @Override
        public AdvancedMessage<E> forEachBodySection(Consumer<Section<E>> consumer) {
            if (!bodySections.isEmpty()) {
                bodySections.forEach(section -> {
                    consumer.accept(section);
                });
            } else {
                if (message.body != null) {
                    consumer.accept(message.body);
                }
            }

            return this;
        }

        @Override
        public AdvancedMessage<E> abort() {
            this.aborted = true;
            return this;
        }

        @Override
        public boolean aborted() {
            return aborted;
        }

        @Override
        public AdvancedMessage<E> complete(boolean complete) {
            this.complete = complete;
            return this;
        }

        @Override
        public boolean complete() {
            return complete;
        }
    }
}
