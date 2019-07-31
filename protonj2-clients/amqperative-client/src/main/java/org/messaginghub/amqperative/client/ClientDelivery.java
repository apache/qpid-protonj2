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
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton4j.amqp.messaging.AmqpValue;
import org.apache.qpid.proton4j.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Footer;
import org.apache.qpid.proton4j.amqp.messaging.Header;
import org.apache.qpid.proton4j.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Properties;
import org.apache.qpid.proton4j.amqp.messaging.Rejected;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Section;
import org.apache.qpid.proton4j.amqp.transactions.TransactionalState;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.engine.IncomingDelivery;
import org.messaginghub.amqperative.Delivery;
import org.messaginghub.amqperative.DeliveryState;
import org.messaginghub.amqperative.Message;
import org.messaginghub.amqperative.Receiver;
import org.messaginghub.amqperative.client.exceptions.ClientExceptionSupport;

/**
 * Client inbound delivery object.
 */
public class ClientDelivery implements Delivery {

    private final ClientReceiver receiver;
    private final IncomingDelivery delivery;

    private Message<?> cachedMessage;

    /**
     * Creates a new client delivery object linked to the given {@link IncomingDelivery}
     * instance.
     *
     * @param receiver
     *      The {@link Receiver} that processed this delivery.
     * @param delivery
     *      The proton incoming delivery that backs this client delivery facade.
     */
    ClientDelivery(ClientReceiver receiver, IncomingDelivery delivery) {
        this.receiver = receiver;
        this.delivery = delivery;
    }

    @Override
    public Message<?> getMessage() throws ClientException {
        if (delivery.isPartial()) {
            // TODO - Client exception of some sort, ClientPartialMessageException etc
            throw new IllegalStateException("Message is still only partially delivered.");
        }

        Message<?> message = cachedMessage;
        if (message == null && delivery.available() > 0) {
            message = decodeMessage(delivery.readAll());
        }

        return message;
    }

    @Override
    public Delivery accept() {
        delivery.disposition(Accepted.getInstance());
        return this;
    }

    @Override
    public Delivery disposition(DeliveryState state, boolean settle) {
        org.apache.qpid.proton4j.amqp.transport.DeliveryState protonState = null;
        if (state != null) {
            // TODO - Create simpler DeliveryState object for client side ?
            switch (state.getType()) {
                case ACCEPTED:
                    protonState = Accepted.getInstance();
                case MODIFIED:
                    protonState = new Modified();
                case REJECTED:
                    protonState = new Rejected();
                case RELEASED:
                    protonState = Released.getInstance();
                case TRANSACTIONAL:
                    protonState = new TransactionalState();
                default:
                    throw new IllegalArgumentException("Unknown DeliveryState type given");
            }
        }

        receiver.disposition(delivery, protonState, settle);
        return this;
    }

    @Override
    public Delivery settle() {
        receiver.disposition(delivery, null, true);
        return this;
    }

    @Override
    public DeliveryState getLocalState() {
        // TODO - Create simple mapping builder in our DeliveryState implementation
        return null;
    }

    @Override
    public DeliveryState getRemoteState() {
        // TODO - Create simple mapping builder in our DeliveryState implementation
        return null;
    }

    @Override
    public boolean isRemotelySettled() {
        return delivery.isRemotelySettled();
    }

    @Override
    public byte[] getTag() {
        return delivery.getTag();
    }

    @Override
    public int getMessageFormat() {
        return delivery.getMessageFormat();
    }

    // TODO - Move to Message Codec helper class

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
