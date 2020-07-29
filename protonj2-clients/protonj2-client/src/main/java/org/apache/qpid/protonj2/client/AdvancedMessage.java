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
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> header(Header header);

    /**
     * Return the current {@link DeliveryAnnotations} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link DeliveryAnnotations} for this message.
     */
    DeliveryAnnotations deliveryAnnotations();

    /**
     * Assign or replace the {@link DeliveryAnnotations} instance associated with this message.
     *
     * @param deliveryAnnotations
     *      The {@link DeliveryAnnotations} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> deliveryAnnotations(DeliveryAnnotations deliveryAnnotations);

    /**
     * Return the current {@link MessageAnnotations} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link MessageAnnotations} for this message.
     */
    MessageAnnotations messageAnnotations();

    /**
     * Assign or replace the {@link MessageAnnotations} instance associated with this message.
     *
     * @param messageAnnotations
     *      The {@link MessageAnnotations} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> messageAnnotations(MessageAnnotations messageAnnotations);

    /**
     * Return the current {@link Properties} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link Properties} for this message.
     */
    Properties properties();

    /**
     * Assign or replace the {@link Properties} instance associated with this message.
     *
     * @param properties
     *      The {@link Properties} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> properties(Properties properties);

    /**
     * Return the current {@link ApplicationProperties} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link ApplicationProperties} for this message.
     */
    ApplicationProperties applicationProperties();

    /**
     * Assign or replace the {@link ApplicationProperties} instance associated with this message.
     *
     * @param applicationProperties
     *      The {@link ApplicationProperties} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> applicationProperties(ApplicationProperties applicationProperties);

    /**
     * Return the current {@link Footer} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link Footer} for this message.
     */
    Footer footer();

    /**
     * Assign or replace the {@link Footer} instance associated with this message.
     *
     * @param footer
     *      The {@link Footer} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> footer(Footer footer);

    /**
     * @return the currently assigned message format for this message.
     */
    int messageFormat();

    /**
     * Sets the message format to use when the message is sent.  The exact structure of a
     * message, together with its encoding, is defined by the message format (default is
     * the AMQP defined message format zero.
     * <p>
     * This field MUST be specified for the first transfer of a multi-transfer message, if
     * it is not set at the time of send of the first transfer the sender uses the AMQP
     * default value of zero for this field.
     * <p>
     * The upper three octets of a message format code identify a particular message format.
     * The lowest octet indicates the version of said message format. Any given version of
     * a format is forwards compatible with all higher versions.
     * <pre>
     *
     *       3 octets      1 octet
     *    +----------------+---------+
     *    | message format | version |
     *    +----------------+---------+
     *    |                          |
     *   msb                        lsb
     *
     * </pre>
     *
     * @param messageFormat
     *      The message format to encode into the transfer frame that carries the message.
     *
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> messageFormat(int messageFormat);

    AdvancedMessage<E> addBodySection(Section<?> bodySection);

    AdvancedMessage<E> bodySections(Collection<Section<?>> sections);

    Collection<Section<?>> bodySections();

    AdvancedMessage<E> forEachBodySection(Consumer<Section<?>> consumer);

    AdvancedMessage<E> clearBodySections();

    /**
     * Marks the currently streaming message as being aborted.
     * <p>
     * Simply marking a {@link AdvancedMessage} as being aborted does not signal
     * the remote peer that the message was aborted, the message must be sent a final
     * time using the {@link Sender} that was used to stream it originally.  A
     * {@link AdvancedMessage} cannot be aborted following a send where the complete
     * flag was set to true (default value).
     *
     * @param aborted
     *      Should the message be marked as having been aborted.
     *
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> abort();

    /**
     * @return true if this message has been marked as aborted previously.
     */
    boolean aborted();

    /**
     * Marks the currently streaming message as being complete (default is <code>true</code>).
     * Any message that is sent with the complete value as <code>false</code> will not be
     * delivered by the remote until it has been sent a final time with the complete flag
     * set to true.
     * <p>
     * Simply marking a {@link AdvancedMessage} as being complete does not signal the
     * remote peer that the message was completed, the message must be sent a final time
     * using the {@link Sender} that was used to send it originally.  A {@link AdvancedMessage}
     * cannot be completed following a message send that was marked as being aborted using the
     * {@link #aborted(boolean)} method.
     *
     * @param complete
     *      Should the next send of this message mark it as being complete.
     *
     * @return this {@link AdvancedMessage} instance.
     */
    AdvancedMessage<E> complete(boolean complete);

    /**
     * @return true if this message has been marked as being the complete.
     */
    boolean complete();

    /**
     * Encodes the Message
     *
     * @return the encoded form of this message in a {@link ProtonBuffer} instance.
     */
    ProtonBuffer encode();

}
