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

import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.Data;

/**
 * Message object that provides a high level abstraction to raw AMQP types
 * <p>
 * TODO - Should we have an AMQPMessage or some such that exposed more of the raw
 * proton types and make this one a much more abstract mapping ?
 * something like toAdvancedMessage or magic cast into something based on type of
 *
 * @param <E> The type of the message body that this message carries
 */
public interface Message<E> {

    // TODO: actual Message interface.
    // Various questions: Have specific body type setters? Allow setting general body section types? Do both? Use a
    // Message builder/factory?
    //
    // public static <E> Message<E> create(Class<E> typeClass);
    //
    public static Message<Void> create() {
        return ClientMessage.create(null, () -> {
            return null;
        });
    }

    public static Message<Object> create(Object body) {
        return ClientMessage.create(body, () -> {
            return new AmqpValue(body);
        });
    }

    public static Message<String> create(String body) {
        return ClientMessage.create(body, () -> {
            return new AmqpValue(body);
        });
    }

    public static Message<byte[]> create(byte[] body) {
        return ClientMessage.create(body, () -> {
            return new Data(new Binary(body));
        });
    }

    public static <E> Message<List<E>> create(List<E> body) {
        return ClientMessage.create(body, () -> {
            return new AmqpValue(body);
        });
    }

    public static <K, V> Message<Map<K, V>> create(Map<K, V> body) {
        return ClientMessage.create(body, () -> {
            return new AmqpValue(body);
        });
    }

    //----- AMQP Header Section

    /**
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

    Object getDeliveryAnnotations(String key);

    boolean hasDeliveryAnnotations(String key);

    Message<E> setDeliveryAnnotation(String key, Object value);

    //----- Message Annotations

    Object getMessageAnnotation(String key);

    boolean hasMessageAnnotation(String key);

    Message<E> setMessageAnnotation(String key, Object value);

    //----- Application Properties

    Object getApplicationProperty(String key);

    boolean hasApplicationProperty(String key);

    Message<E> setApplicationProperty(String key, Object value);

    //----- Footer

    Object getFooter(String key);

    boolean hasFooter(String key);

    Message<E> setFooter(String key, Object value);

    //----- AMQP Body Section

    /**
     * Returns the body that is conveyed in this message or null if no body was set locally
     * or sent from the remote if this is an incoming message.
     *
     * @return the message body or null if none present.
     */
    E body();

}
