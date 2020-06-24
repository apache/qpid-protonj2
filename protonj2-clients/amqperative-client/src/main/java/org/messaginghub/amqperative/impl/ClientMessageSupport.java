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
package org.messaginghub.amqperative.impl;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecFactory;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.Encoder;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.types.Binary;
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
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.exceptions.ClientException;

/**
 * Support methods dealing with Message types and encode or decode operations.
 */
abstract class ClientMessageSupport {

    private static final Encoder DEFAULT_ENCODER = CodecFactory.getDefaultEncoder();
    private static final Decoder DEFAULT_DECODER = CodecFactory.getDefaultDecoder();

    //----- Message Encoding

    public static ProtonBuffer encodeMessage(ClientMessage<?> message) {
        return encodeMessage(DEFAULT_ENCODER, DEFAULT_ENCODER.newEncoderState(), ProtonByteBufferAllocator.DEFAULT, message);
    }

    public static ProtonBuffer encodeMessage(Encoder encoder, ProtonBufferAllocator allocator, ClientMessage<?> message) {
        return encodeMessage(encoder, encoder.newEncoderState(), ProtonByteBufferAllocator.DEFAULT, message);
    }

    public static ProtonBuffer encodeMessage(Encoder encoder, EncoderState encoderState, ProtonBufferAllocator allocator, ClientMessage<?> message) {
        ProtonBuffer buffer = allocator.allocate();

        Header header = message.getHeader();
        DeliveryAnnotations deliveryAnnotations = message.getDeliveryAnnotations();
        MessageAnnotations messageAnnotations = message.getMessageAnnotations();
        Properties properties = message.getProperties();
        ApplicationProperties applicationProperties = message.getApplicationProperties();
        Section body = message.getBodySection();
        Footer footer = message.getFooter();

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
        if (body != null) {
            encoder.writeObject(buffer, encoderState, body);
        }
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
        Section body = null;
        Footer footer = null;
        Section section = null;

        while (buffer.isReadable()) {
            try {
                section = (Section) decoder.readObject(buffer, decoderState);
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
            result.setHeader(header);
            result.setDeliveryAnnotations(deliveryAnnotations);
            result.setMessageAnnotations(messageAnnotations);
            result.setProperties(properties);
            result.setApplicationProperties(applicationProperties);
            result.setFooter(footer);

            return result;
        }

        throw new ClientException("Failed to create Message from encoded payload");
    }

    private static ClientMessage<?> createMessageFromBodySection(Section body) {
        Message<?> result = null;
        if (body == null) {
            result = Message.create();
        } else if (body instanceof Data) {
            Binary payload = ((Data) body).getValue();
            if (payload != null) {
                // TODO - Offset ?
                result = Message.create(payload.getArray());
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
}
