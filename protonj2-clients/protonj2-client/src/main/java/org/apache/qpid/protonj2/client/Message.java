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
package org.apache.qpid.protonj2.client;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.client.impl.ClientMessageSupport;
import org.apache.qpid.protonj2.client.impl.ClientStreamableMessage;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Message object that provides a high level abstraction to raw AMQP types
 *
 * @param <E> The type of the message body that this message carries
 */
public interface Message<E> {

    /**
     * Create and return an {@link Message} that will carry no body {@link Section}.
     *
     * @return a new {@link Message} instance with an empty body {@link Section}.
     */
    static Message<Void> create() {
        return ClientMessage.create(null, () -> {
            return null;
        });
    }

    /**
     * Create and return an {@link Message} that will wrap the given {@link Object} in
     * an {@link AmqpValue} section.
     *
     * @param body
     *      An object that will be wrapped in an {@link AmqpValue} body section.
     *
     * @return a new {@link Message} instance with a body containing the given value.
     */
    static Message<Object> create(Object body) {
        return ClientMessage.create(body, () -> {
            return new AmqpValue(body);
        });
    }

    /**
     * Create and return an {@link Message} that will wrap the given {@link String} in
     * an {@link AmqpValue} section.
     *
     * @param body
     *      An String value that will be wrapped in an {@link AmqpValue} body section.
     *
     * @return a new {@link Message} instance with a body containing the given string value.
     */
    static Message<String> create(String body) {
        return ClientMessage.create(body, () -> {
            return new AmqpValue(body);
        });
    }

    /**
     * Create and return an {@link Message} that will wrap the given byte array in
     * an {@link Data} section.
     *
     * @param body
     *      An byte array that will be wrapped in an {@link Data} body section.
     *
     * @return a new {@link Message} instance with a body containing the given byte array.
     */
    static Message<byte[]> create(byte[] body) {
        return ClientMessage.create(body, () -> {
            return new Data(new Binary(body));
        });
    }

    /**
     * Create and return an {@link Message} that will wrap the given {@link List} in
     * an {@link AmqpSequence} section.
     *
     * @param body
     *      An List that will be wrapped in an {@link AmqpSequence} body section.
     *
     * @return a new {@link Message} instance with a body containing the given List.
     */
    static <E> Message<List<E>> create(List<E> body) {
        return ClientMessage.create(body, () -> {
            return new AmqpSequence(body);
        });
    }

    /**
     * Create and return an {@link Message} that will wrap the given {@link Map} in
     * an {@link AmqpValue} section.
     *
     * @param body
     *      An Map that will be wrapped in an {@link AmqpValue} body section.
     *
     * @return a new {@link Message} instance with a body containing the given Map.
     */
    static <K, V> Message<Map<K, V>> create(Map<K, V> body) {
        return ClientMessage.create(body, () -> {
            return new AmqpValue(body);
        });
    }

    static StreamableMessage createStreamableMessage() {
        return new ClientStreamableMessage();
    }

    //----- Message specific APIs

    /**
     * Safely convert this {@link Message} instance into an {@link AdvancedMessage} reference
     * which can offer more low level APIs to an experienced client user.
     *
     * @return a {@link AdvancedMessage} that contains this message's current state.
     *
     * @throws UnsupportedOperationException if the {@link Message} implementation cannot be converted
     */
    default AdvancedMessage<E> toAdvancedMessage() {
        if (this instanceof AdvancedMessage) {
            return (AdvancedMessage<E>) this;
        } else {
            return ClientMessageSupport.convertMessage(this);
        }
    }

    //----- AMQP Header Section

    /**
     * For an message being sent this method returns the current state of the
     * durable flag on the message.  For a received message this method returns
     * the durable flag value at the time of sending (or false if not set) unless
     * the value is updated after being received by the receiver.
     *
     * @return true if the Message is marked as being durable
     */
    boolean durable();

    /**
     * Controls if the message is marked as durable when sent.
     *
     * @param durable
     *      value assigned to the durable flag for this message.
     *
     * @return this message for chaining.
     */
    Message<E> durable(boolean durable);

    /**
     * @return the currently configured priority or the default if none set.
     */
    byte priority();

    /**
     * Sets the relative message priority.  Higher numbers indicate higher priority messages.
     * Messages with higher priorities MAY be delivered before those with lower priorities.
     *
     * @param priority
     * 		The priority value to assign this message.
     *
     * @return this message instance.
     */
    Message<E> priority(byte priority);

    /**
     * @return the currently set Time To Live duration (milliseconds).
     */
    long timeToLive();

    Message<E> timeToLive(long timeToLive);

    /**
     * @return if this message has been acquired by another link previously
     */
    boolean firstAcquirer();

    Message<E> firstAcquirer(boolean firstAcquirer);

    /**
     * @return the number of failed delivery attempts that this message has been part of.
     */
    long deliveryCount();

    Message<E> deliveryCount(long deliveryCount);

    //----- AMQP Properties Section

    /**
     * @return the currently set Message ID or null if none set.
     */
    Object messageId();

    Message<E> messageId(Object messageId);

    /**
     * @return the currently set User ID or null if none set.
     */
    byte[] userId();

    Message<E> userId(byte[] userId);

    /**
     * @return the currently set 'To' address which indicates the intended destination of the message.
     */
    String to();

    Message<E> to(String to);

    /**
     * @return the currently set subject metadata for this message or null if none set.
     */
    String subject();

    Message<E> subject(String subject);

    /**
     * @return the configured address of the node where replies to this message should be sent, or null if not set.
     */
    String replyTo();

    Message<E> replyTo(String replyTo);

    /**
     * @return the currently assigned correlation ID or null if none set.
     */
    Object correlationId();

    Message<E> correlationId(Object correlationId);

    /**
     * @return the assigned content type value for the message body section or null if not set.
     */
    String contentType();

    Message<E> contentType(String contentType);

    /**
     * @return the assigned content encoding value for the message body section or null if not set.
     */
    String contentEncoding();

    Message<?> contentEncoding(String contentEncoding);

    /**
     * @return the configured absolute time of expiration for this message.
     */
    long absoluteExpiryTime();

    Message<E> absoluteExpiryTime(long expiryTime);

    /**
     * @return the absolute time of creation for this message.
     */
    long creationTime();

    Message<E> creationTime(long createTime);

    /**
     * @return the assigned group ID for this message or null if not set.
     */
    String groupId();

    Message<E> groupId(String groupId);

    /**
     * @return the assigned group sequence for this message.
     */
    int groupSequence();

    Message<E> groupSequence(int groupSequence);

    /**
     * @return the client-specific id that is used so that client can send replies to this message to a specific group.
     */
    String replyToGroupId();

    Message<E> replyToGroupId(String replyToGroupId);

    //----- Delivery Annotations

    Object deliveryAnnotation(String key);

    boolean hasDeliveryAnnotation(String key);

    boolean hasDeliveryAnnotations();

    Object removeDeliveryAnnotation(String key);

    Message<E> forEachDeliveryAnnotation(BiConsumer<String, Object> action);

    Message<E> deliveryAnnotation(String key, Object value);

    //----- Message Annotations

    Object messageAnnotation(String key);

    boolean hasMessageAnnotation(String key);

    boolean hasMessageAnnotations();

    Object removeMessageAnnotation(String key);

    Message<E> forEachMessageAnnotation(BiConsumer<String, Object> action);

    Message<E> messageAnnotation(String key, Object value);

    //----- Application Properties

    Object applicationProperty(String key);

    boolean hasApplicationProperty(String key);

    boolean hasApplicationProperties();

    Object removeApplicationProperty(String key);

    Message<E> forEachApplicationProperty(BiConsumer<String, Object> action);

    Message<E> applicationProperty(String key, Object value);

    //----- Footer

    Object footer(String key);

    boolean hasFooter(String key);

    boolean hasFooters();

    Object removeFooter(String key);

    Message<E> forEachFooter(BiConsumer<String, Object> action);

    Message<E> footer(String key, Object value);

    //----- AMQP Body Section

    /**
     * Returns the body that is conveyed in this message or null if no body was set locally
     * or sent from the remote if this is an incoming message.
     *
     * @return the message body or null if none present.
     *
     * @throws UnsupportedOperationException if the implementation can't provide a body directly.
     */
    E body();

}
