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
import org.apache.qpid.proton4j.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton4j.amqp.messaging.AmqpValue;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.messaginghub.amqperative.client.ClientMessage;

/**
 * Message object that provides a high level abstraction to raw AMQP types
 * <p>
 * TODO - Should we have an AMQPMessage or some such that exposed more of the raw
 * proton types and make this one a much more abstract mapping ?
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

    public static Message<List<Object>> create(List<Object> body) {
        return ClientMessage.create(body, () -> {
            return new AmqpSequence(body);
        });
    }

    public static Message<Map<Object, Object>> create(Map<Object, Object> body) {
        return ClientMessage.create(body, () -> {
            return new AmqpValue(body);
        });
    }

    //----- AMQP Header Section

    /**
     * @return true if the Message is marked as being durable
     */
    boolean isDurable();

    /**
     * Controls if the message is marked as durable when sent.
     *
     * @param durable
     *      value assigned to the durable flag for this message.
     *
     * @return this message for chaining.
     */
    Message<E> setDurable(boolean durable);

    /**
     * @return the currently configured priority or the default if none set.
     */
    byte getPriority();

    /**
     * Sets the relative message priority.  Higher numbers indicate higher priority messages.
     * Messages with higher priorities MAY be delivered before those with lower priorities.
     *
     * @param priority
     * 		The priority value to assign this message.
     *
     * @return this message instance.
     */
    Message<E> setPriority(byte priority);

    long getTimeToLive();

    Message<E> setTimeToLive(long timeToLive);

    boolean getFirstAcquirer();

    Message<E> setFirstAcquirer(boolean firstAcquirer);

    long getDeliveryCount();

    Message<E> setDeliveryCount(long deliveryCount);

    //----- AMQP Properties Section

    Object getMessageId();

    Message<?> setMessageId(Object messageId);

    byte[] getUserId();

    Message<?> setUserId(byte[] userId);

    String getTo();

    Message<?> setTo(String to);

    String getSubject();

    Message<?> setSubject(String subject);

    String getReplyTo();

    Message<?> setReplyTo(String replyTo);

    Object getCorrelationId();

    Message<?> setCorrelationId(Object correlationId);

    String getContentType();

    Message<?> setContentType(String contentType);

    String getContentEncoding();

    Message<?> setContentEncoding(String contentEncoding);

    long getAbsoluteExpiryTime();

    Message<?> setAbsoluteExpiryTime(long expiryTime);

    long getCreationTime();

    Message<?> setCreationTime(long createTime);

    String getGroupId();

    Message<?> setGroupId(String groupId);

    int getGroupSequence();

    Message<?> setGroupSequence(int groupSequence);

    String getReplyToGroupId();

    Message<?> setReplyToGroupId(String replyToGroupId);

    //----- AMQP Body Section

    /**
     * Returns the body that is conveyed in this message or null if no body was set locally
     * or sent from the remote if this is an incoming message.
     *
     * @return the message body or null if none present.
     */
    E getBody();

}
