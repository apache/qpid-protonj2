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
import java.util.Map;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
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
        return ClientMessage.createAdvancedMessage();
    }

    /**
     * Return the current {@link Header} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link Header} for this message.
     *
     * @throws ClientException if an error occurs while retrieving the message data.
     */
    Header header() throws ClientException;

    /**
     * Assign or replace the {@link Header} instance associated with this message.
     *
     * @param header
     *      The {@link Header} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while writing the message {@link Header} value.
     */
    AdvancedMessage<E> header(Header header) throws ClientException;

    /**
     * Return the current {@link MessageAnnotations} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link MessageAnnotations} for this message.
     *
     * @throws ClientException if an error occurs while retrieving the message data.
     */
    MessageAnnotations annotations() throws ClientException;

    /**
     * Assign or replace the {@link MessageAnnotations} instance associated with this message.
     *
     * @param messageAnnotations
     *      The {@link MessageAnnotations} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while writing the message {@link MessageAnnotations} value.
     */
    AdvancedMessage<E> annotations(MessageAnnotations messageAnnotations) throws ClientException;

    /**
     * Return the current {@link Properties} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link Properties} for this message.
     *
     * @throws ClientException if an error occurs while retrieving the message data.
     */
    Properties properties() throws ClientException;

    /**
     * Assign or replace the {@link Properties} instance associated with this message.
     *
     * @param properties
     *      The {@link Properties} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while writing the message {@link Properties} value.
     */
    AdvancedMessage<E> properties(Properties properties) throws ClientException;

    /**
     * Return the current {@link ApplicationProperties} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link ApplicationProperties} for this message.
     *
     * @throws ClientException if an error occurs while retrieving the message data.
     */
    ApplicationProperties applicationProperties() throws ClientException;

    /**
     * Assign or replace the {@link ApplicationProperties} instance associated with this message.
     *
     * @param applicationProperties
     *      The {@link ApplicationProperties} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while writing the message {@link ApplicationProperties} value.
     */
    AdvancedMessage<E> applicationProperties(ApplicationProperties applicationProperties) throws ClientException;

    /**
     * Return the current {@link Footer} assigned to this message, if none was assigned yet
     * then this method returns <code>null</code>.
     *
     * @return the currently assigned {@link Footer} for this message.
     *
     * @throws ClientException if an error occurs while retrieving the message data.
     */
    Footer footer() throws ClientException;

    /**
     * Assign or replace the {@link Footer} instance associated with this message.
     *
     * @param footer
     *      The {@link Footer} value to assign to this message.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while writing the message {@link Footer} value.
     */
    AdvancedMessage<E> footer(Footer footer) throws ClientException;

    /**
     * @return the currently assigned message format for this message.
     *
     * @throws ClientException if an error occurs while retrieving the message data.
     */
    int messageFormat() throws ClientException;

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
     *
     * @throws ClientException if an error occurs while configuring the message format.
     */
    AdvancedMessage<E> messageFormat(int messageFormat) throws ClientException;

    /**
     * Adds the given {@link Section} to the internal collection of sections that will be sent
     * to the remote peer when this message is encoded.  If a previous section was added by a call
     * to the {@link Message#body(Object)} method it should be retained as the first element of
     * the running list of body sections contained in this message.
     * <p>
     * The implementation should make an attempt to validate that sections added are valid for
     * the message format that is assigned when they are added.
     *
     * @param bodySection
     *      The {@link Section} instance to append to the internal collection.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while writing to the message body sections.
     */
    AdvancedMessage<E> addBodySection(Section<?> bodySection) throws ClientException;

    /**
     * Sets the body {@link Section} instances to use when encoding this message.  The value
     * given replaces any existing sections assigned to this message through the {@link Message#body(Object)}
     * or {@link AdvancedMessage#addBodySection(Section)} methods.  Calling this method with a null
     * or empty collection is equivalent to calling the {@link #clearBodySections()} method.
     *
     * @param sections
     *      The {@link Collection} of {@link Section} instance to assign this message.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while writing to the message body sections.
     */
    AdvancedMessage<E> bodySections(Collection<Section<?>> sections) throws ClientException;

    /**
     * Create and return an unmodifiable {@link Collection} that contains the {@link Section} instances
     * currently assigned to this message.  Changes to this message body after calling this will not be
     * reflected in the returned collection.
     *
     * @return an unmodifiable {@link Collection} that is a view of the current sections assigned to this message.
     *
     * @throws ClientException if an error occurs while retrieving the message data.
     */
    Collection<Section<?>> bodySections() throws ClientException;

    /**
     * Performs the given action for each body {@link Section} of the {@link AdvancedMessage} until all
     * sections have been presented to the given {@link Consumer} or the consumer throws an exception.
     *
     * @param consumer
     *      the {@link Consumer} that will operate on each of the body sections in this message.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while iterating over the message data.
     */
    AdvancedMessage<E> forEachBodySection(Consumer<Section<?>> consumer) throws ClientException;

    /**
     * Clears all current body {@link Section} elements from the {@link AdvancedMessage}.
     *
     * @return this {@link AdvancedMessage} instance.
     *
     * @throws ClientException if an error occurs while clearing the message body sections.
     */
    AdvancedMessage<E> clearBodySections() throws ClientException;

    /**
     * Encodes the {@link AdvancedMessage} for transmission by the client.  The provided {@link DeliveryAnnotations}
     * can be included or augmented by the {@link AdvancedMessage} implementation based on the target message format.
     * The implementation is responsible for ensuring that the delivery annotations are treated correctly encoded into
     * the correct location in the message.
     *
     * @param deliveryAnnotations
     *      A {@link Map} of delivery annotation values that were requested to be included in the transmitted message.
     *
     * @return the encoded form of this message in a {@link ProtonBuffer} instance.
     *
     * @throws ClientException if an error occurs while encoding the message data.
     */
    default ProtonBuffer encode(Map<String, Object> deliveryAnnotations) throws ClientException {
        return encode(deliveryAnnotations, ProtonBufferAllocator.defaultAllocator());
    }

    /**
     * Encodes the {@link AdvancedMessage} for transmission by the client.  The provided {@link DeliveryAnnotations}
     * can be included or augmented by the {@link AdvancedMessage} implementation based on the target message format.
     * The implementation is responsible for ensuring that the delivery annotations are treated correctly encoded into
     * the correct location in the message.
     *
     * @param deliveryAnnotations
     *      A {@link Map} of delivery annotation values that were requested to be included in the transmitted message.
     * @param allocator
     * 		An allocator that should be used to create the buffer the message is encoded into.
     *
     * @return the encoded form of this message in a {@link ProtonBuffer} instance.
     *
     * @throws ClientException if an error occurs while encoding the message data.
     */
    ProtonBuffer encode(Map<String, Object> deliveryAnnotations, ProtonBufferAllocator allocator) throws ClientException;

}
