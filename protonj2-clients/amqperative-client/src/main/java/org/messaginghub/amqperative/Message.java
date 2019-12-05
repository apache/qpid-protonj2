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
package org.messaginghub.amqperative;

import java.util.List;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.AmqpValue;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.messaginghub.amqperative.impl.ClientMessage;

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

    long timeToLive();

    Message<E> timeToLive(long timeToLive);

    boolean firstAcquirer();

    Message<E> firstAcquirer(boolean firstAcquirer);

    long deliveryCount();

    Message<E> deliveryCount(long deliveryCount);

    //----- AMQP Properties Section

    Object messageId();

    Message<E> messageId(Object messageId);

    byte[] userId();

    Message<E> userId(byte[] userId);

    String to();

    Message<E> to(String to);

    String subject();

    Message<E> subject(String subject);

    String replyTo();

    Message<E> replyTo(String replyTo);

    Object correlationId();

    Message<E> correlationId(Object correlationId);

    String contentType();

    Message<E> contentType(String contentType);

    String contentEncoding();

    Message<?> contentEncoding(String contentEncoding);

    long absoluteExpiryTime();

    Message<E> absoluteExpiryTime(long expiryTime);

    long creationTime();

    Message<E> creationTime(long createTime);

    String groupId();

    Message<E> groupId(String groupId);

    int groupSequence();

    Message<E> groupSequence(int groupSequence);

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
