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

import java.util.Collection;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.client.impl.ClientMessage;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;

/**
 * Advanced AMQP Message object that provides a thin abstraction to raw AMQP types
 *
 * @param <E> The type of the message body that this message carries
 */
public interface AdvancedMessage<E> extends Message<E> {

    /**
     * Creates a new {@link AdvancedMessage} instance using the library default implementation.
     *
     * @param <V> The type to use when specifying the body section value type.
     *
     * @return a new {@link AdvancedMessage} instance.
     */
    static <V> AdvancedMessage<V> create() {
        return ClientMessage.createAdvanvedMessage();
    }

    /**
     * Return the current {@link Header} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link Header} for this message.
     */
    Header header();

    /**
     * Assign or replace the {@link Header} instance associated with this message.
     *
     * @param header
     *      The {@link Header} value to assign to this message.
     *
     * @return this message instance.
     */
    AdvancedMessage<E> header(Header header);

    /**
     * Return the current {@link DeliveryAnnotations} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link DeliveryAnnotations} for this message.
     */
    DeliveryAnnotations deliveryAnnotations();

    AdvancedMessage<E> deliveryAnnotations(DeliveryAnnotations deliveryAnnotations);

    /**
     * Return the current {@link MessageAnnotations} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link MessageAnnotations} for this message.
     */
    MessageAnnotations messageAnnotations();

    AdvancedMessage<E> messageAnnotations(MessageAnnotations messageAnnotations);

    /**
     * Return the current {@link Properties} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link Properties} for this message.
     */
    Properties properties();

    AdvancedMessage<E> properties(Properties properties);

    /**
     * Return the current {@link ApplicationProperties} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link ApplicationProperties} for this message.
     */
    ApplicationProperties applicationProperties();

    AdvancedMessage<E> applicationProperties(ApplicationProperties applicationProperties);

    /**
     * Return the current {@link Footer} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link Footer} for this message.
     */
    Footer footer();

    AdvancedMessage<E> footer(Footer footer);

    int messageFormat();

    AdvancedMessage<E> messageFormat(int messageFormat);

    ProtonBuffer encode();

    AdvancedMessage<E> addBodySection(Section<E> bodySection);

    AdvancedMessage<E> bodySections(Collection<Section<E>> sections);

    Collection<Section<E>> bodySections();

    AdvancedMessage<E> forEachBodySection(Consumer<Section<E>> consumer);

}
