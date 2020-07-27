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

import java.util.function.BiFunction;

import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * Options class that controls various aspects of a {@link MessageOutputStream} instance.
 */
public class MessageOutputStreamOptions {

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Footer footer;
    private int messageFormat;
    private int outputLimit;

    private BiFunction<Footer, Integer, Footer> footerFinalizationHandler;

    /**
     * Creates a {@link MessageOutputStreamOptions} instance with default values for all options
     */
    public MessageOutputStreamOptions() {
    }

    /**
     * Create a {@link MessageOutputStreamOptions} instance that copies all configuration from the given
     * {@link MessageOutputStreamOptions} instance.
     *
     * @param options
     *      The options instance to copy all configuration values from.
     */
    public MessageOutputStreamOptions(MessageOutputStreamOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link MessageOutputStreamOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link MessageOutputStreamOptions} class for chaining.
     */
    protected MessageOutputStreamOptions copyInto(MessageOutputStreamOptions other) {
        other.header(header != null ? header.copy() : null);
        other.deliveryAnnotations(deliveryAnnotations != null ? deliveryAnnotations.copy() : null);
        other.messageAnnotations(messageAnnotations != null ? messageAnnotations.copy() : null);
        other.properties(properties != null ? properties.copy() : null);
        other.applicationProperties(applicationProperties != null ? applicationProperties.copy() : null);
        other.footer(footer != null ? footer.copy() : null);
        other.messageFormat(messageFormat);
        other.outputLimit(outputLimit);

        return this;
    }

    /**
     * @return The assigned AMQP {@link Header} instance that will be written as part of the streamed output.
     */
    public Header header() {
        return header;
    }

    /**
     * Sets the AMQP {@link Header} instance to send as part of the streamed message payload.  If a previous
     * value was set it is overwritten by the new value (or cleared if null).  Any changes to this value
     * after the first flush of streamed data will not have no affect.
     *
     * @param header
     *      The AMQP {@link Header} to write with the streamed message payload.
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions header(Header header) {
        this.header = header;
        return this;
    }

    /**
     * @return The assigned AMQP {@link DeliveryAnnotations} instance that will be written as part of the streamed output.
     */
    public DeliveryAnnotations deliveryAnnotations() {
        return deliveryAnnotations;
    }

    /**
     * Sets the AMQP {@link DeliveryAnnotations} instance to send as part of the streamed message payload.  If a previous
     * value was set it is overwritten by the new value (or cleared if null).  Any changes to this value
     * after the first flush of streamed data will not have no affect.
     *
     * @param deliveryAnnotations
     *      The AMQP {@link DeliveryAnnotations} to write with the streamed message payload.
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions deliveryAnnotations(DeliveryAnnotations deliveryAnnotations) {
        this.deliveryAnnotations = deliveryAnnotations;
        return this;
    }

    /**
     * @return The assigned AMQP {@link MessageAnnotations} instance that will be written as part of the streamed output.
     */
    public MessageAnnotations messageAnnotations() {
        return messageAnnotations;
    }

    /**
     * Sets the AMQP {@link MessageAnnotations} instance to send as part of the streamed message payload.  If a previous
     * value was set it is overwritten by the new value (or cleared if null).  Any changes to this value
     * after the first flush of streamed data will not have no affect.
     *
     * @param messageAnnotations
     *      The AMQP {@link MessageAnnotations} to write with the streamed message payload.
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions messageAnnotations(MessageAnnotations messageAnnotations) {
        this.messageAnnotations = messageAnnotations;
        return this;
    }

    /**
     * @return The assigned AMQP {@link Properties} instance that will be written as part of the streamed output.
     */
    public Properties properties() {
        return properties;
    }

    /**
     * Sets the AMQP {@link Properties} instance to send as part of the streamed message payload.  If a previous
     * value was set it is overwritten by the new value (or cleared if null).  Any changes to this value
     * after the first flush of streamed data will not have no affect.
     *
     * @param properties
     *      The AMQP {@link Properties} to write with the streamed message payload.
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions properties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * @return The assigned AMQP {@link ApplicationProperties} instance that will be written as part of the streamed output.
     */
    public ApplicationProperties applicationProperties() {
        return applicationProperties;
    }

    /**
     * Sets the AMQP {@link ApplicationProperties} instance to send as part of the streamed message payload.  If a previous
     * value was set it is overwritten by the new value (or cleared if null).  Any changes to this value
     * after the first flush of streamed data will not have no affect.
     *
     * @param applicationProperties
     *      The AMQP {@link ApplicationProperties} to write with the streamed message payload.
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions applicationProperties(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
        return this;
    }

    /**
     * @return The assigned AMQP {@link Footer} instance that will be written as part of the streamed output.
     */
    public Footer footer() {
        return footer;
    }

    /**
     * Sets the AMQP {@link Footer} instance to send as part of the streamed message payload.  If a previous
     * value was set it is overwritten by the new value (or cleared if null).  Any changes to this value
     * after the first flush of streamed data will not have no affect.
     *
     * @param footer
     *      The AMQP {@link Footer} to write with the streamed message payload.
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions footer(Footer footer) {
        this.footer = footer;
        return this;
    }

    /**
     * Returns the configured message format value that will be set on the first outgoing
     * AMQP {@link Transfer} frame for the delivery that comprises this streamed message.
     *
     * @return the configured message format that will be sent.
     */
    public int messageFormat() {
        return messageFormat;
    }

    /**
     * Sets the message format value to use when writing the first AMQP {@link Transfer} frame
     * for this streamed message.  If not set the default value (0) is used for the message.
     *
     * @param messageFormat
     *      The message format value to use when streaming the message data.
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions messageFormat(int messageFormat) {
        this.messageFormat = messageFormat;
        return this;
    }

    /**
     * @return the configured output size limit for associated {@link MessageOutputStream}
     */
    public int outputLimit() {
        return outputLimit;
    }

    /**
     * Sets the overall output limit for this associated {@link MessageOutputStream} that
     * the options are applied to.
     *
     * @param outputLimit
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions outputLimit(int outputLimit) {
        this.outputLimit = outputLimit;
        return this;
    }

    /**
     * @return the configured footer finalization handler to call when all streamed bytes have been written.
     */
    public BiFunction<Footer, Integer, Footer> footerFinalizationEvent() {
        return footerFinalizationHandler;
    }

    /**
     * Allow hook to fill in Footer with checksum or other data based on the data written.  The
     * {@link BiFunction} given is provided with the configured {@link Footer} instance and the
     * count of bytes written and the returned Footer value will be encoded as part of the final
     * {@link Transfer} frame for this message.
     *
     * @param handler
     *      Handler that is called prior to final Message write with count of bytes written.
     *
     * @return this {@link MessageOutputStreamOptions} instance.
     */
    public MessageOutputStreamOptions footerFinalizationEvent(BiFunction<Footer, Integer, Footer> handler) {
        this.footerFinalizationHandler = handler;
        return this;
    }
}