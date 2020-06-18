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
package org.apache.qpid.proton4j.test.driver.actions;

import java.util.Date;
import java.util.List;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.codec.messaging.AmqpSequence;
import org.apache.qpid.proton4j.test.driver.codec.messaging.AmqpValue;
import org.apache.qpid.proton4j.test.driver.codec.messaging.ApplicationProperties;
import org.apache.qpid.proton4j.test.driver.codec.messaging.Data;
import org.apache.qpid.proton4j.test.driver.codec.messaging.DeliveryAnnotations;
import org.apache.qpid.proton4j.test.driver.codec.messaging.Footer;
import org.apache.qpid.proton4j.test.driver.codec.messaging.Header;
import org.apache.qpid.proton4j.test.driver.codec.messaging.MessageAnnotations;
import org.apache.qpid.proton4j.test.driver.codec.messaging.Properties;
import org.apache.qpid.proton4j.test.driver.codec.transport.Transfer;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.DescribedType;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedByte;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.apache.qpid.proton4j.types.transport.ReceiverSettleMode;

/**
 * AMQP Close injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public final class TransferInjectAction extends AbstractPerformativeInjectAction<Transfer> {

    private final Transfer transfer = new Transfer();

    private ProtonBuffer payload;

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private DescribedType body;
    private Footer footer;

    public TransferInjectAction(AMQPTestDriver driver) {
        super(driver);
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
    protected void beforeActionPerformed(AMQPTestDriver driver) {
        // We fill in a channel using the next available channel id if one isn't set, then
        // report the outbound begin to the session so it can track this new session.
        if (onChannel() == CHANNEL_UNSET) {
            onChannel(driver.getSessions().getLastOpenedSession().getLocalChannel().intValue());
        }

        // Auto select last opened receiver on last opened session.  Later an option could
        // be added to allow forcing the handle to be null for testing specification requirements.
        if (transfer.getHandle() == null) {
            transfer.setHandle(driver.getSessions().getLastOpenedSession().getLastOpenedReceiver().getHandle());
        }

        // Here we could check if the delivery Id is set and if not grab a valid
        // next Id from the driver as well as checking for a session and using last
        // created one if none set.
    }

    public TransferInjectAction withHandle(long handle) {
        transfer.setHandle(UnsignedInteger.valueOf(handle));
        return this;
    }

    public TransferInjectAction withDeliveryId(int deliveryId) {
        transfer.setDeliveryId(UnsignedInteger.valueOf(deliveryId));
        return this;
    }

    public TransferInjectAction withDeliveryId(long deliveryId) {
        transfer.setDeliveryId(UnsignedInteger.valueOf(deliveryId));
        return this;
    }

    public TransferInjectAction withDeliveryTag(byte[] deliveryTag) {
        transfer.setDeliveryTag(new Binary(deliveryTag));
        return this;
    }

    public TransferInjectAction withDeliveryTag(Binary deliveryTag) {
        transfer.setDeliveryTag(deliveryTag);
        return this;
    }

    public TransferInjectAction withMessageFormat(long messageFormat) {
        transfer.setMessageFormat(UnsignedInteger.valueOf(messageFormat));
        return this;
    }

    public TransferInjectAction withSettled(Boolean settled) {
        transfer.setSettled(settled);
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
        transfer.setRcvSettleMode(rcvSettleMode.getValue());
        return this;
    }

    public TransferInjectAction withState(DeliveryState state) {
        transfer.setState(TypeMapper.mapFromProtonType(state));
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

    public TransferInjectAction withPayload(byte[] payload) {
        this.payload = ProtonByteBufferAllocator.DEFAULT.wrap(payload);
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
            deliveryAnnotations = new DeliveryAnnotations();
        }
        return deliveryAnnotations;
    }

    private MessageAnnotations getOrCreateMessageAnnotations() {
        if (messageAnnotations == null) {
            messageAnnotations = new MessageAnnotations();
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
            applicationProperties = new ApplicationProperties();
        }
        return applicationProperties;
    }

    private Footer getOrCreateFooter() {
        if (footer == null) {
            footer = new Footer();
        }
        return footer;
    }

    private ProtonBuffer encodePayload() {
        org.apache.qpid.proton4j.test.driver.codec.Codec codec =
            org.apache.qpid.proton4j.test.driver.codec.Codec.Factory.create();
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        if (header != null) {
            codec.putDescribedType(header);
        }
        if (deliveryAnnotations != null) {
            codec.putDescribedType(deliveryAnnotations);
        }
        if (messageAnnotations != null) {
            codec.putDescribedType(messageAnnotations);
        }
        if (properties != null) {
            codec.putDescribedType(properties);
        }
        if (applicationProperties != null) {
            codec.putDescribedType(applicationProperties);
        }
        if (body != null) {
            codec.putDescribedType(body);
        }
        if (footer != null) {
            codec.putDescribedType(footer);
        }

        codec.encode(buffer);

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
            getOrCreateHeader().setPriority(UnsignedByte.valueOf(priority));
            return this;
        }

        public HeaderBuilder withTimeToLive(long ttl) {
            getOrCreateHeader().setTtl(UnsignedInteger.valueOf(ttl));
            return this;
        }

        public HeaderBuilder withFirstAcquirer(boolean first) {
            getOrCreateHeader().setFirstAcquirer(first);
            return this;
        }

        public HeaderBuilder withDeliveryCount(long count) {
            getOrCreateHeader().setDeliveryCount(UnsignedInteger.valueOf(count));
            return this;
        }
    }

    public final class DeliveryAnnotationsBuilder extends SectionBuilder {

        public DeliveryAnnotationsBuilder withAnnotation(String key, Object value) {
            getOrCreateDeliveryAnnotations().setSymbolKeyedAnnotation(key, value);
            return this;
        }

        public DeliveryAnnotationsBuilder withAnnotation(Symbol key, Object value) {
            getOrCreateDeliveryAnnotations().setSymbolKeyedAnnotation(key, value);
            return this;
        }
    }

    public final class MessageAnnotationsBuilder extends SectionBuilder {

        public MessageAnnotationsBuilder withAnnotation(String key, Object value) {
            getOrCreateMessageAnnotations().setSymbolKeyedAnnotation(key, value);
            return this;
        }

        public MessageAnnotationsBuilder withAnnotation(Symbol key, Object value) {
            getOrCreateMessageAnnotations().setSymbolKeyedAnnotation(key, value);
            return this;
        }
    }

    public final class PropertiesBuilder extends SectionBuilder {

        public PropertiesBuilder withMessageId(Object value) {
            getOrCreateProperties().setMessageId(value);
            return this;
        }

        public PropertiesBuilder withUserID(Binary value) {
            getOrCreateProperties().setUserId(value);
            return this;
        }

        public PropertiesBuilder withTo(String value) {
            getOrCreateProperties().setTo(value);
            return this;
        }

        public PropertiesBuilder withSubject(String value) {
            getOrCreateProperties().setSubject(value);
            return this;
        }

        public PropertiesBuilder withReplyTp(String value) {
            getOrCreateProperties().setReplyTo(value);
            return this;
        }

        public PropertiesBuilder withCorrelationId(Object value) {
            getOrCreateProperties().setCorrelationId(value);
            return this;
        }

        public PropertiesBuilder withContentType(String value) {
            getOrCreateProperties().setContentType(Symbol.valueOf(value));
            return this;
        }

        public PropertiesBuilder withContentType(Symbol value) {
            getOrCreateProperties().setContentType(value);
            return this;
        }

        public PropertiesBuilder withContentEncoding(String value) {
            getOrCreateProperties().setContentEncoding(Symbol.valueOf(value));
            return this;
        }

        public PropertiesBuilder withContentEncoding(Symbol value) {
            getOrCreateProperties().setContentEncoding(value);
            return this;
        }

        public PropertiesBuilder withAbsoluteExpiryTime(long value) {
            getOrCreateProperties().setAbsoluteExpiryTime(new Date(value));
            return this;
        }

        public PropertiesBuilder withCreationTime(long value) {
            getOrCreateProperties().setCreationTime(new Date(value));
            return this;
        }

        public PropertiesBuilder withGroupId(String value) {
            getOrCreateProperties().setGroupId(value);
            return this;
        }

        public PropertiesBuilder withGroupSequence(long value) {
            getOrCreateProperties().setGroupSequence(UnsignedInteger.valueOf(value));
            return this;
        }

        public PropertiesBuilder withReplyToGroupId(String value) {
            getOrCreateProperties().setReplyToGroupId(value);
            return this;
        }
    }

    public final class ApplicationPropertiesBuilder extends SectionBuilder {

        public ApplicationPropertiesBuilder withApplicationProperty(String key, Object value) {
            getOrCreateApplicationProperties().setApplicationProperty(key, value);
            return this;
        }
    }

    public final class BodySectionBuilder extends SectionBuilder {

        public BodySectionBuilder withString(String body) {
            TransferInjectAction.this.body = new AmqpValue(body);
            return this;
        }

        public BodySectionBuilder withData(byte[] body) {
            TransferInjectAction.this.body = new Data(new Binary(body));
            return this;
        }

        public BodySectionBuilder withData(Binary body) {
            TransferInjectAction.this.body = new Data(body);
            return this;
        }

        public BodySectionBuilder withSequence(List<Object> sequence) {
            TransferInjectAction.this.body = new AmqpSequence(sequence);
            return this;
        }
    }

    public final class FooterBuilder extends SectionBuilder {

        public FooterBuilder withFooter(Object key, Object value) {
            getOrCreateFooter().setFooterProperty(key, value);
            return this;
        }
    }
}
