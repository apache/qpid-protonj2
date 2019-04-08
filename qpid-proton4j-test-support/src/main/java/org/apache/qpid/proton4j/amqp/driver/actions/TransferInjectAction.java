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
package org.apache.qpid.proton4j.amqp.driver.actions;

import java.util.LinkedHashMap;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.messaging.AmqpValue;
import org.apache.qpid.proton4j.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.amqp.messaging.Data;
import org.apache.qpid.proton4j.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Footer;
import org.apache.qpid.proton4j.amqp.messaging.Header;
import org.apache.qpid.proton4j.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton4j.amqp.messaging.Properties;
import org.apache.qpid.proton4j.amqp.messaging.Section;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecFactory;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;

/**
 * AMQP Close injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public final class TransferInjectAction extends AbstractPerformativeInjectAction<Transfer> {

    private final Transfer transfer;

    private ProtonBuffer payload;

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private Section body;
    private Footer footer;

    public TransferInjectAction(Transfer transfer) {
        this.transfer = transfer;
    }

    @Override
    public Transfer getPerformative() {
        return transfer;
    }

    @Override
    public ProtonBuffer getPayload() {
        if (payload == null) {
            payload = encodePayload();
        }
        return payload;
    }

    @Override
    public void perform(AMQPTestDriver driver) {
        // Here we could check if the delivery Id is set and if not grab a valid
        // next Id from the driver as well as checking for a session and using last
        // created one if none set.
        driver.sendAMQPFrame(onChannel(), getPerformative(), getPayload());
    }

    public TransferInjectAction withHandle(long handle) {
        transfer.setHandle(handle);
        return this;
    }

    public TransferInjectAction withDeliveryId(long deliveryId) {
        transfer.setDeliveryId(deliveryId);
        return this;
    }

    public TransferInjectAction withDeliveryTag(Binary deliveryTag) {
        transfer.setDeliveryTag(deliveryTag);
        return this;
    }

    public TransferInjectAction withMessageFormat(long messageFormat) {
        transfer.setMessageFormat(messageFormat);
        return this;
    }

    public TransferInjectAction withSettled(boolean settled) {
        transfer.setSettled(settled);
        return this;
    }

    public TransferInjectAction withMore(boolean more) {
        transfer.setMore(more);
        return this;
    }

    public TransferInjectAction withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        transfer.setRcvSettleMode(rcvSettleMode);
        return this;
    }

    public TransferInjectAction withState(DeliveryState state) {
        transfer.setState(state);
        return this;
    }

    public TransferInjectAction withResume(boolean resume) {
        transfer.setResume(resume);
        return this;
    }

    public TransferInjectAction withAborted(boolean aborted) {
        transfer.setAborted(aborted);
        return this;
    }

    public TransferInjectAction withBatchable(boolean batchable) {
        transfer.setBatchable(batchable);
        return this;
    }

    public TransferInjectAction withPayload(ProtonBuffer payload) {
        this.payload = payload;
        return this;
    }

    //----- Allow easy building of an AMQP message in the payload

    public HeaderBuilder withHeader() {
        return new HeaderBuilder();
    }

    public DeliveryAnnotationsBuilder withDeliveryAnnotations() {
        return new DeliveryAnnotationsBuilder();
    }

    public MessageAnnotationsBuilder withMessageAnnotations() {
        return new MessageAnnotationsBuilder();
    }

    public PropertiesBuilder withProperties() {
        return new PropertiesBuilder();
    }

    public ApplicationPropertiesBuilder withApplicationProperties() {
        return new ApplicationPropertiesBuilder();
    }

    public BodySectionBuilder withBody() {
        return new BodySectionBuilder();
    }

    public FooterBuilder withFooter() {
        return new FooterBuilder();
    }

    private Header getOrCreateHeader() {
        if (header == null) {
            header = new Header();
        }
        return header;
    }

    private DeliveryAnnotations getOrCreateDeliveryAnnotations() {
        if (deliveryAnnotations == null) {
            deliveryAnnotations = new DeliveryAnnotations(new LinkedHashMap<>());
        }
        return deliveryAnnotations;
    }

    private MessageAnnotations getOrCreateMessageAnnotations() {
        if (messageAnnotations == null) {
            messageAnnotations = new MessageAnnotations(new LinkedHashMap<>());
        }
        return messageAnnotations;
    }

    private Properties getOrCreateProperties() {
        if (properties == null) {
            properties = new Properties();
        }
        return properties;
    }

    private ApplicationProperties getOrCreateApplicationProperties() {
        if (applicationProperties == null) {
            applicationProperties = new ApplicationProperties(new LinkedHashMap<>());
        }
        return applicationProperties;
    }

    private Footer getOrCreateFooter() {
        if (footer == null) {
            footer = new Footer(new LinkedHashMap<>());
        }
        return footer;
    }

    private ProtonBuffer encodePayload() {
        Encoder encoder = CodecFactory.getDefaultEncoder();
        EncoderState encoderState = encoder.newEncoderState();
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

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

    protected abstract class SectionBuilder {

        public TransferInjectAction also() {
            return TransferInjectAction.this;
        }
    }

    public final class HeaderBuilder extends SectionBuilder {

        public HeaderBuilder withDurability(boolean durable) {
            getOrCreateHeader().setDurable(durable);
            return this;
        }

        public HeaderBuilder withPriority(byte priority) {
            getOrCreateHeader().setPriority(priority);
            return this;
        }

        public HeaderBuilder withTimeToLive(long ttl) {
            getOrCreateHeader().setTimeToLive(ttl);
            return this;
        }

        public HeaderBuilder withFirstAcquirer(boolean first) {
            getOrCreateHeader().setFirstAcquirer(first);
            return this;
        }

        public HeaderBuilder withDeliveryCount(long count) {
            getOrCreateHeader().setDeliveryCount(count);
            return this;
        }
    }

    public final class DeliveryAnnotationsBuilder extends SectionBuilder {

        DeliveryAnnotationsBuilder withAnnotation(Symbol key, Object value) {
            getOrCreateDeliveryAnnotations().getValue().put(key, value);
            return this;
        }
    }

    public final class MessageAnnotationsBuilder extends SectionBuilder {

        MessageAnnotationsBuilder withAnnotation(Symbol key, Object value) {
            getOrCreateMessageAnnotations().getValue().put(key, value);
            return this;
        }
    }

    public final class PropertiesBuilder extends SectionBuilder {

        PropertiesBuilder withMessageId(Object value) {
            getOrCreateProperties().setMessageId(value);
            return this;
        }

        PropertiesBuilder withUserID(Binary value) {
            getOrCreateProperties().setUserId(value);
            return this;
        }

        PropertiesBuilder withTo(String value) {
            getOrCreateProperties().setTo(value);
            return this;
        }

        PropertiesBuilder withSubject(String value) {
            getOrCreateProperties().setSubject(value);
            return this;
        }

        PropertiesBuilder withReplyTp(String value) {
            getOrCreateProperties().setReplyTo(value);
            return this;
        }

        PropertiesBuilder withCorrelationId(Object value) {
            getOrCreateProperties().setCorrelationId(value);
            return this;
        }

        PropertiesBuilder withContentType(String value) {
            getOrCreateProperties().setContentType(value);
            return this;
        }

        PropertiesBuilder withContentEncoding(String value) {
            getOrCreateProperties().setContentEncoding(value);
            return this;
        }

        PropertiesBuilder withAbsoluteExpiryTime(long value) {
            getOrCreateProperties().setAbsoluteExpiryTime(value);
            return this;
        }

        PropertiesBuilder withCreationTime(long value) {
            getOrCreateProperties().setCreationTime(value);
            return this;
        }

        PropertiesBuilder withGroupId(String value) {
            getOrCreateProperties().setGroupId(value);
            return this;
        }

        PropertiesBuilder withGroupSequence(long value) {
            getOrCreateProperties().setGroupSequence(value);
            return this;
        }

        PropertiesBuilder withReplyToGroupId(String value) {
            getOrCreateProperties().setReplyToGroupId(value);
            return this;
        }
    }

    public final class ApplicationPropertiesBuilder extends SectionBuilder {

        ApplicationPropertiesBuilder withApplicationProperty(String key, Object value) {
            getOrCreateApplicationProperties().getValue().put(key, value);
            return this;
        }
    }

    public final class BodySectionBuilder extends SectionBuilder {

        // Other methods can be added to expand on the types that can go into the body

        public BodySectionBuilder withString(String body) {
            TransferInjectAction.this.body = new AmqpValue(body);
            return this;
        }

        public BodySectionBuilder withData(Binary body) {
            TransferInjectAction.this.body = new Data(body);
            return this;
        }

        public BodySectionBuilder withSection(Section body) {
            TransferInjectAction.this.body = body;
            return this;
        }
    }

    public final class FooterBuilder extends SectionBuilder {

        FooterBuilder withFooter(Object key, Object value) {
            getOrCreateFooter().getValue().put(key, value);
            return this;
        }
    }
}
