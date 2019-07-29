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

import java.util.function.Supplier;

import org.apache.qpid.proton4j.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Footer;
import org.apache.qpid.proton4j.amqp.messaging.Header;
import org.apache.qpid.proton4j.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Properties;
import org.apache.qpid.proton4j.amqp.messaging.Section;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.messaginghub.amqperative.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientMessage<E> implements Message<E> {

    private static final Logger LOG = LoggerFactory.getLogger(ClientMessage.class);

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Footer footer;

    private final Supplier<Section> sectionSupplier;
    private E body;

    /**
     * Create a new {@link ClientMessage} instance with a {@link Supplier} that will
     * provide the AMQP {@link Section} value for any body that is set on the message.
     *
     * @param sectionSupplier
     *      A {@link Supplier} that will generate Section values for the message body.
     */
    ClientMessage(Supplier<Section> sectionSupplier) {
        this.sectionSupplier = sectionSupplier;
    }

    public static Message<String> create(String body, Supplier<Section> sectionSupplier) {
        return new ClientMessage<String>(sectionSupplier).setBody(body);
    }

    //----- Message Header API

    @Override
    public Message<E> setDurable(boolean durable) {
        if (header == null) {
            header = new Header();
        }

        header.setDurable(durable);

        return this;
    }

    @Override
    public boolean isDurable() {
        // TODO Auto-generated method stub
        return header == null ? false : header.isDurable();
    }

    //----- Message body access

    @Override
    public E getBody() {
        return body;
    }

    //----- Access to proton resources

    Header getHeader() {
        return header;
    }

    DeliveryAnnotations getDeliveryAnnotations() {
        return deliveryAnnotations;
    }

    MessageAnnotations getMessageAnnotations() {
        return messageAnnotations;
    }

    Properties getProperties() {
        return properties;
    }

    ApplicationProperties getApplicationProperties() {
        return applicationProperties;
    }

    Section getBodySection() {
        return sectionSupplier.get();
    }

    Footer getFooter() {
        return footer;
    }

    //----- Internal API

    ClientMessage<E> setBody(E body) {
        this.body = body;
        return this;
    }

    public static ProtonBuffer encodeMessage(ClientMessage<?> message) {
        // TODO - Hand in the Engine and use configured allocator and or cached encoder
        Encoder encoder = CodecFactory.getDefaultEncoder();
        EncoderState encoderState = encoder.newEncoderState();
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate(4096);

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
}
