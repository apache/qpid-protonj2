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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.SectionEncoder;
import org.apache.qpid.protonj2.engine.util.StringUtils;
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

    private static final SectionEncoder SECTION_ENCODER = new SectionEncoder(DEFAULT_ENCODER);

    private static final int DEFAULT_BUFFER_ALLOCATION = 256;

    private static final ThreadLocal<EncoderState> THREAD_LOCAL_ENCODER_STATE =
        ThreadLocal.withInitial(() -> DEFAULT_ENCODER.newEncoderState());
    private static final ThreadLocal<DecoderState> THREAD_LOCAL_DECODER_STATE =
            ThreadLocal.withInitial(() -> DEFAULT_DECODER.newDecoderState());

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
     *
     * @throws ClientException if an unrecoverable error occurs during message conversion.
     */
    public static <E> AdvancedMessage<E> convertMessage(Message<E> message) throws ClientException {
        if (message instanceof AdvancedMessage) {
            return (AdvancedMessage<E>) message;
        } else {
            try {
                return message.toAdvancedMessage();
            } catch (UnsupportedOperationException uoe) {
                return convertFromOutsideMessage(message);
            }
        }
    }

    //----- Message Encoding

    public static ProtonBuffer encodeSection(Section<?> section, ProtonBuffer buffer) {
        DEFAULT_ENCODER.writeObject(buffer, DEFAULT_ENCODER.newEncoderState(), section);
        return buffer;
    }

    //----- Message Encoding

    public static ProtonBuffer encodeMessage(AdvancedMessage<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        return encodeMessage(DEFAULT_ENCODER, THREAD_LOCAL_ENCODER_STATE.get(), ProtonBufferAllocator.defaultAllocator(), message, deliveryAnnotations);
    }

    public static ProtonBuffer encodeMessage(AdvancedMessage<?> message, Map<String, Object> deliveryAnnotations, ProtonBufferAllocator allocator) throws ClientException {
        return encodeMessage(DEFAULT_ENCODER, THREAD_LOCAL_ENCODER_STATE.get(), allocator, message, deliveryAnnotations);
    }

    public static ProtonBuffer encodeMessage(Encoder encoder, ProtonBufferAllocator allocator, AdvancedMessage<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        return encodeMessage(encoder, encoder.newEncoderState(), allocator, message, deliveryAnnotations);
    }

    public static ProtonBuffer encodeMessage(Encoder encoder, EncoderState encoderState, ProtonBufferAllocator allocator, AdvancedMessage<?> message, Map<String, Object> deliveryAnnotations) throws ClientException {
        ProtonBuffer buffer = allocator.outputBuffer(DEFAULT_BUFFER_ALLOCATION);

        Header header = message.header();
        MessageAnnotations messageAnnotations = message.annotations();
        Properties properties = message.properties();
        ApplicationProperties applicationProperties = message.applicationProperties();
        Footer footer = message.footer();

        if (header != null) {
            SECTION_ENCODER.write(buffer, header);
        }
        if (deliveryAnnotations != null) {
            SECTION_ENCODER.write(buffer, new DeliveryAnnotations(StringUtils.toSymbolKeyedMap(deliveryAnnotations)));
        }
        if (messageAnnotations != null) {
            SECTION_ENCODER.write(buffer, messageAnnotations);
        }
        if (properties != null) {
            SECTION_ENCODER.write(buffer, properties);
        }
        if (applicationProperties != null) {
            SECTION_ENCODER.write(buffer, applicationProperties);
        }

        message.forEachBodySection(section -> SECTION_ENCODER.write(buffer, section));

        if (footer != null) {
            SECTION_ENCODER.write(buffer, footer);
        }

        return buffer.convertToReadOnly();
    }

    //----- Message Decoding

    public static Message<?> decodeMessage(ProtonBuffer buffer, Consumer<DeliveryAnnotations> daConsumer) throws ClientException {
        return decodeMessage(DEFAULT_DECODER, THREAD_LOCAL_DECODER_STATE.get(), buffer, daConsumer);
    }

    public static Message<?> decodeMessage(Decoder decoder, ProtonBuffer buffer, Consumer<DeliveryAnnotations> daConsumer) throws ClientException {
        return decodeMessage(decoder, decoder.newDecoderState(), buffer, daConsumer);
    }

    public static Message<?> decodeMessage(Decoder decoder, DecoderState decoderState,
                                           ProtonBuffer buffer, Consumer<DeliveryAnnotations> daConsumer) throws ClientException {

        final ClientMessage<?> message = new ClientMessage<>();

        Section<?> section = null;

        while (buffer.isReadable()) {
            try {
                section = (Section<?>) decoder.readObject(buffer, decoderState);
            } catch (Exception e) {
                throw ClientExceptionSupport.createNonFatalOrPassthrough(e);
            }

            switch (section.getType()) {
                case Header:
                    message.header((Header) section);
                    break;
                case DeliveryAnnotations:
                    if (daConsumer != null) {
                        daConsumer.accept((DeliveryAnnotations) section);
                    }
                    break;
                case MessageAnnotations:
                    message.annotations((MessageAnnotations) section);
                    break;
                case Properties:
                    message.properties((Properties) section);
                    break;
                case ApplicationProperties:
                    message.applicationProperties((ApplicationProperties) section);
                    break;
                case Data:
                case AmqpSequence:
                case AmqpValue:
                    message.addBodySection(section);
                    break;
                case Footer:
                    message.footer((Footer) section);
                    break;
                default:
                    throw new ClientException("Unknown Message Section forced decode abort.");
            }
        }

        return message;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <E> Section<E> createSectionFromValue(E body) {
        if (body == null) {
            return null;
        } else if (body instanceof byte[]) {
            return (Section<E>) new Data((byte[]) body);
        } else if (body instanceof List){
            return new AmqpSequence((List) body);
        } else {
            return new AmqpValue(body);
        }
    }

    //----- Internal Implementation

    private static <E> ClientMessage<E> convertFromOutsideMessage(Message<E> source) throws ClientException {
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

        final MessageAnnotations messageAnnotations;
        if (source.hasAnnotations()) {
            messageAnnotations = new MessageAnnotations(new LinkedHashMap<>());

            source.forEachAnnotation((key, value) -> {
                messageAnnotations.getValue().put(Symbol.valueOf(key), value);
            });
        } else {
            messageAnnotations = null;
        }

        final ApplicationProperties applicationProperties;
        if (source.hasProperties()) {
            applicationProperties = new ApplicationProperties(new LinkedHashMap<>());

            source.forEachProperty((key, value) -> {
                applicationProperties.getValue().put(key, value);
            });
        } else {
            applicationProperties = null;
        }

        final Footer footer;
        if (source.hasFooters()) {
            footer = new Footer(new LinkedHashMap<>());

            source.forEachFooter((key, value) -> {
                footer.getValue().put(Symbol.valueOf(key), value);
            });
        } else {
            footer = null;
        }

        ClientMessage<E> message = new ClientMessage<>(createSectionFromValue(source.body()));

        message.header(header);
        message.properties(properties);
        message.annotations(messageAnnotations);
        message.applicationProperties(applicationProperties);
        message.footer(footer);

        return message;
    }
}
