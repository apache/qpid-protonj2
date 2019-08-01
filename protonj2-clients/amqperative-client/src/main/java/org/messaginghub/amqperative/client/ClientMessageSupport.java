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

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton4j.amqp.messaging.AmqpValue;
import org.apache.qpid.proton4j.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Footer;
import org.apache.qpid.proton4j.amqp.messaging.Header;
import org.apache.qpid.proton4j.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Properties;
import org.apache.qpid.proton4j.amqp.messaging.Section;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.client.exceptions.ClientExceptionSupport;

/**
 * Support methods dealing with Message types and encode or decode operations.
 */
abstract class ClientMessageSupport {

    //----- Message Encoding

    public static ProtonBuffer encodeMessage(ClientMessage<?> message) {
        return encodeMessage(CodecFactory.getDefaultEncoder(), ProtonByteBufferAllocator.DEFAULT, message);
    }

    public static ProtonBuffer encodeMessage(Encoder encoder, ProtonBufferAllocator allocator, ClientMessage<?> message) {
        // TODO - Hand in the Engine and use configured allocator and or cached encoder
        EncoderState encoderState = encoder.newEncoderState();
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
        Decoder decoder = CodecFactory.getDefaultDecoder();
        DecoderState state = decoder.newDecoderState();

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
                section = (Section) decoder.readObject(buffer, state);
            } catch (IOException e) {
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
