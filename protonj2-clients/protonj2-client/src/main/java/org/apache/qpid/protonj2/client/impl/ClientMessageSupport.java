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

import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.types.Binary;
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

/**
 * Support methods dealing with Message types and encode or decode operations.
 */
public abstract class ClientMessageSupport {

    private static final Encoder DEFAULT_ENCODER = CodecFactory.getDefaultEncoder();
    private static final Decoder DEFAULT_DECODER = CodecFactory.getDefaultDecoder();

    //----- Message Conversion

    /**
     * Converts a {@link Message} instance into a {@link ClientMessage} instance
     * either by cast or by construction of a new instance with a copy of the
     * values carried in the given message.
     *
     * @param <E> the body type of the given message.
     *
     * @param message
     *      The {@link Message} type to attempt to convert to a {@link ClientMessage} instance.
     *
     * @return a {@link ClientMessage} that represents the given {@link Message} instance.
     */
    public static <E> ClientMessage<E> convertMessage(Message<E> message) {
        if (message instanceof ClientMessage) {
            return (ClientMessage<E>) message;
        } else {
            return convertFromOutsideMessage(message);
        }
    }

    //----- Message Encoding

    public static ProtonBuffer encodeMessage(ClientMessage<?> message) {
        return encodeMessage(DEFAULT_ENCODER, DEFAULT_ENCODER.newEncoderState(), ProtonByteBufferAllocator.DEFAULT, message);
    }

    public static ProtonBuffer encodeMessage(Encoder encoder, ProtonBufferAllocator allocator, ClientMessage<?> message) {
        return encodeMessage(encoder, encoder.newEncoderState(), ProtonByteBufferAllocator.DEFAULT, message);
    }

    public static ProtonBuffer encodeMessage(Encoder encoder, EncoderState encoderState, ProtonBufferAllocator allocator, ClientMessage<?> message) {
        ProtonBuffer buffer = allocator.allocate();

        Header header = message.header();
        DeliveryAnnotations deliveryAnnotations = message.deliveryAnnotations();
        MessageAnnotations messageAnnotations = message.messageAnnotations();
        Properties properties = message.properties();
        ApplicationProperties applicationProperties = message.applicationProperties();
        Footer footer = message.footer();

        if (header != null) {
            encoder.writeObject(buffer, encoderState, header);
        }
        if (deliveryAnnotations != null) {
            encoder.writeObject(buffer, encoderState, deliveryAnnotations);
        }
        if (messageAnnotations != null) {
            encoder.writeObject(buffer, encoderState, messageAnnotations);
        }
        if (properties != null) {
            encoder.writeObject(buffer, encoderState, properties);
        }
        if (applicationProperties != null) {
            encoder.writeObject(buffer, encoderState, applicationProperties);
        }

        message.forEachBodySection(section -> encoder.writeObject(buffer, encoderState, section));

        if (footer != null) {
            encoder.writeObject(buffer, encoderState, footer);
        }

        return buffer;
    }

    //----- Message Decoding

    public static Message<?> decodeMessage(ProtonBuffer buffer) throws ClientException {
        return decodeMessage(DEFAULT_DECODER, DEFAULT_DECODER.newDecoderState(), buffer);
    }

    public static Message<?> decodeMessage(Decoder decoder, ProtonBuffer buffer) throws ClientException {
        return decodeMessage(decoder, decoder.newDecoderState(), buffer);
    }

    public static Message<?> decodeMessage(Decoder decoder, DecoderState decoderState, ProtonBuffer buffer) throws ClientException {
        Header header = null;
        DeliveryAnnotations deliveryAnnotations = null;
        MessageAnnotations messageAnnotations = null;
        Properties properties = null;
        ApplicationProperties applicationProperties = null;
        Section<?> body = null;
        Footer footer = null;
        Section<?> section = null;

        while (buffer.isReadable()) {
            try {
                section = (Section<?>) decoder.readObject(buffer, decoderState);
            } catch (Exception e) {
                throw ClientExceptionSupport.createNonFatalOrPassthrough(e);
            }

            switch (section.getType()) {
                case Header:
                    header = (Header) section;
                    break;
                case DeliveryAnnotations:
                    deliveryAnnotations = (DeliveryAnnotations) section;
                    break;
                case MessageAnnotations:
                    messageAnnotations = (MessageAnnotations) section;
                    break;
                case Properties:
                    properties = (Properties) section;
                    break;
                case ApplicationProperties:
                    applicationProperties = (ApplicationProperties) section;
                    break;
                case Data:
                case AmqpSequence:
                case AmqpValue:
                    body = section;
                    break;
                case Footer:
                    footer = (Footer) section;
                    break;
                default:
                    throw new ClientException("Unknown Message Section forced decode abort.");
            }
        }

        ClientMessage<?> result = createMessageFromBodySection(body);

        if (result != null) {
            result.header(header);
            result.deliveryAnnotations(deliveryAnnotations);
            result.messageAnnotations(messageAnnotations);
            result.properties(properties);
            result.applicationProperties(applicationProperties);
            result.footer(footer);

            return result;
        }

        throw new ClientException("Failed to create Message from encoded payload");
    }

    //----- Internal Implementation

    private static ClientMessage<?> createMessageFromBodySection(Section<?> body) {
        Message<?> result = null;
        if (body == null) {
            result = Message.create();
        } else if (body instanceof Data) {
            byte[] payload = ((Data) body).getValue();
            if (payload != null) {
                result = Message.create(payload);
            }
        } else if (body instanceof AmqpSequence) {
            result = Message.create(((AmqpSequence) body).getValue());
        } else if (body instanceof AmqpValue) {
            Object value = ((AmqpValue) body).getValue();
            if (value instanceof String) {
                result = Message.create((String) value);
            } else {
                result = Message.create(value);
            }
        }

        return (ClientMessage<?>) result;
    }

    private static <E> ClientMessage<E> convertFromOutsideMessage(Message<E> source) {
        Header header = new Header();
        header.setDurable(source.durable());
        header.setPriority(source.priority());
        header.setTimeToLive(source.timeToLive());
        header.setFirstAcquirer(source.firstAcquirer());
        header.setDeliveryCount(source.deliveryCount());

        Properties properties = new Properties();
        properties.setMessageId(source.messageId());
        properties.setUserId(source.userId() != null ? new Binary(source.userId()) : null);
        properties.setTo(source.to());
        properties.setSubject(source.subject());
        properties.setReplyTo(source.replyTo());
        properties.setCorrelationId(source.correlationId());
        properties.setContentType(source.contentType());
        properties.setContentEncoding(source.contentEncoding());
        properties.setAbsoluteExpiryTime(source.absoluteExpiryTime());
        properties.setCreationTime(source.creationTime());
        properties.setGroupId(source.groupId());
        properties.setGroupSequence(source.groupSequence());
        properties.setReplyToGroupId(source.replyToGroupId());

        final DeliveryAnnotations deliveryAnnotations;
        if (source.hasDeliveryAnnotations()) {
            deliveryAnnotations = new DeliveryAnnotations(new HashMap<>());

            source.forEachDeliveryAnnotation((key, value) -> {
                deliveryAnnotations.getValue().put(Symbol.valueOf(key), value);
            });
        } else {
            deliveryAnnotations = null;
        }

        final MessageAnnotations messageAnnotations;
        if (source.hasDeliveryAnnotations()) {
            messageAnnotations = new MessageAnnotations(new HashMap<>());

            source.forEachMessageAnnotation((key, value) -> {
                messageAnnotations.getValue().put(Symbol.valueOf(key), value);
            });
        } else {
            messageAnnotations = null;
        }

        final ApplicationProperties applicationProperties;
        if (source.hasDeliveryAnnotations()) {
            applicationProperties = new ApplicationProperties(new HashMap<>());

            source.forEachMessageAnnotation((key, value) -> {
                applicationProperties.getValue().put(key, value);
            });
        } else {
            applicationProperties = null;
        }

        final Footer footer;
        if (source.hasDeliveryAnnotations()) {
            footer = new Footer(new HashMap<>());

            source.forEachMessageAnnotation((key, value) -> {
                footer.getValue().put(Symbol.valueOf(key), value);
            });
        } else {
            footer = null;
        }

        ClientMessage<E> message = new ClientMessage<>(source.body(), sectionSupplier(source.body()));

        message.header(header);
        message.properties(properties);
        message.deliveryAnnotations(deliveryAnnotations);
        message.messageAnnotations(messageAnnotations);
        message.applicationProperties(applicationProperties);
        message.footer(footer);

        return message;
    }

    @SuppressWarnings("rawtypes")
    private static <E> Supplier<Section<?>> sectionSupplier(E body) {
        if (body == null) {
            return () -> null;
        } else if (body instanceof byte[]) {
            return () -> new Data((byte[]) body);
        } else if (body instanceof List){
            return () -> new AmqpSequence((List) body);
        } else {
            return () -> new AmqpValue(body);
        }
    }
}
