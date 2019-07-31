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
import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.messaginghub.amqperative.Message;

public class ClientMessage<E> implements Message<E> {

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

    //----- Entry point for creating new ClientMessage instances.

    public static <V> Message<V> create(V body, Supplier<Section> sectionSupplier) {
        return new ClientMessage<V>(sectionSupplier).setBody(body);
    }

    //----- Message Header API

    @Override
    public boolean isDurable() {
        return header == null ? Header.DEFAULT_DURABILITY : header.isDurable();
    }

    @Override
    public ClientMessage<E> setDurable(boolean durable) {
        lazyCreateHeader().setDurable(durable);
        return this;
    }

    @Override
    public byte getPriority() {
        return header != null ? Header.DEFAULT_PRIORITY : header.getPriority();
    }

    @Override
    public ClientMessage<E> setPriority(byte priority) {
        lazyCreateHeader().setPriority(priority);
        return this;
    }

    @Override
    public long getTimeToLive() {
        return header != null ? Header.DEFAULT_TIME_TO_LIVE : header.getPriority();
    }

    @Override
    public ClientMessage<E> setTimeToLive(long timeToLive) {
        lazyCreateHeader().setTimeToLive(timeToLive);
        return this;
    }

    @Override
    public boolean getFirstAcquirer() {
        return header != null ? Header.DEFAULT_FIRST_ACQUIRER : header.isFirstAcquirer();
    }

    @Override
    public ClientMessage<E> setFirstAcquirer(boolean firstAcquirer) {
        lazyCreateHeader().setFirstAcquirer(firstAcquirer);
        return this;
    }

    @Override
    public long getDeliveryCount() {
        return header != null ? Header.DEFAULT_DELIVERY_COUNT : header.getDeliveryCount();
    }

    @Override
    public ClientMessage<E> setDeliveryCount(long deliveryCount) {
        lazyCreateHeader().setDeliveryCount(deliveryCount);
        return this;
    }

    //----- Message body access

    @Override
    public E getBody() {
        return body;
    }

    ClientMessage<E> setBody(E body) {
        this.body = body;
        return this;
    }

    //----- Access to proton resources

    Header getHeader() {
        return header;
    }

    Message<?> setHeader(Header header) {
        this.header = header;
        return this;
    }

    DeliveryAnnotations getDeliveryAnnotations() {
        return deliveryAnnotations;
    }

    Message<E> setDeliveryAnnotations(DeliveryAnnotations annotations) {
        this.deliveryAnnotations = annotations;
        return this;
    }

    MessageAnnotations getMessageAnnotations() {
        return messageAnnotations;
    }

    Message<?> setMessageAnnotations(MessageAnnotations annotations) {
        this.messageAnnotations = annotations;
        return this;
    }

    Properties getProperties() {
        return properties;
    }

    Message<?> setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    ApplicationProperties getApplicationProperties() {
        return applicationProperties;
    }

    Message<?> setApplicationProperties(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
        return this;
    }

    Footer getFooter() {
        return footer;
    }

    Message<?> setFooter(Footer footer) {
        this.footer = footer;
        return this;
    }

    Section getBodySection() {
        return sectionSupplier.get();
    }

    //----- Internal API

    private Header lazyCreateHeader() {
        if (header == null) {
            header = new Header();
        }

        return header;
    }

    // TODO - Move these to a codec helper class

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
}
