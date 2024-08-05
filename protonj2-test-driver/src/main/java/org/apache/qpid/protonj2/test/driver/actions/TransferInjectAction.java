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
package org.apache.qpid.protonj2.test.driver.actions;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.SessionTracker;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Accepted;
import org.apache.qpid.protonj2.test.driver.codec.messaging.AmqpSequence;
import org.apache.qpid.protonj2.test.driver.codec.messaging.AmqpValue;
import org.apache.qpid.protonj2.test.driver.codec.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.codec.messaging.DeliveryAnnotations;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Footer;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Header;
import org.apache.qpid.protonj2.test.driver.codec.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Modified;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Properties;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Rejected;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Released;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedByte;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transactions.TransactionalState;
import org.apache.qpid.protonj2.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.protonj2.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.protonj2.test.driver.codec.transport.ReceiverSettleMode;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;

/**
 * AMQP Close injection action which can be added to a driver for write at a specific time or
 * following on from some other action in the test script.
 */
public class TransferInjectAction extends AbstractPerformativeInjectAction<Transfer> {

    private final Transfer transfer = new Transfer();
    private final DeliveryStateBuilder stateBuilder = new DeliveryStateBuilder();

    private ByteBuffer payload;

    private Header header;
    private DeliveryAnnotations deliveryAnnotations;
    private MessageAnnotations messageAnnotations;
    private Properties properties;
    private ApplicationProperties applicationProperties;
    private DescribedType body;
    private Footer footer;

    private boolean explicitlyNullDeliveryTag;

    public TransferInjectAction(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public Transfer getPerformative() {
        return transfer;
    }

    @Override
    public ByteBuffer getPayload() {
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
            onChannel(driver.sessions().getLastLocallyOpenedSession().getLocalChannel().intValue());
        }

        // Auto select last opened receiver on last opened session.  Later an option could
        // be added to allow forcing the handle to be null for testing specification requirements.
        if (transfer.getHandle() == null) {
            transfer.setHandle(driver.sessions().getLastLocallyOpenedSession().getLastOpenedSender().getHandle());
        }

        final SessionTracker session = driver.sessions().getSessionFromLocalChannel(UnsignedShort.valueOf(onChannel()));

        if (transfer.getDeliveryTag() == null && !explicitlyNullDeliveryTag) {
            transfer.setDeliveryTag(new Binary(generateUniqueDeliveryTag()));
        }

        // A test might be trying to send Transfer outside of session scope to check for error handling
        // of unexpected performatives so we just allow no session cases and send what we are told.
        if (session != null) {
            // Here we could check if the delivery Id is set and if not grab a valid
            // next Id from the driver as well as checking for a session and using last
            // created one if none set.

            session.handleLocalTransfer(transfer, getPayload());
        }
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
        explicitlyNullDeliveryTag = deliveryTag == null;
        transfer.setDeliveryTag(new Binary(deliveryTag));
        return this;
    }

    public TransferInjectAction withDeliveryTag(Binary deliveryTag) {
        explicitlyNullDeliveryTag = deliveryTag == null;
        transfer.setDeliveryTag(deliveryTag);
        return this;
    }

    public TransferInjectAction withNullDeliveryTag() {
        explicitlyNullDeliveryTag = true;
        transfer.setDeliveryTag(null);
        return this;
    }

    public TransferInjectAction withMessageFormat(int messageFormat) {
        transfer.setMessageFormat(UnsignedInteger.valueOf(messageFormat));
        return this;
    }

    public TransferInjectAction withMessageFormat(long messageFormat) {
        transfer.setMessageFormat(UnsignedInteger.valueOf(messageFormat));
        return this;
    }

    public TransferInjectAction withMessageFormat(UnsignedInteger messageFormat) {
        transfer.setMessageFormat(messageFormat);
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
        transfer.setState(state);
        return this;
    }

    public DeliveryStateBuilder withState() {
        return stateBuilder;
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
        this.payload = ByteBuffer.allocate(payload.length);
        this.payload.put(payload);
        this.payload.flip().asReadOnlyBuffer();

        return this;
    }

    public TransferInjectAction withPayload(ByteBuffer payload) {
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

    public MessageBuilder withMessage() {
        return new MessageBuilder();
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

    private ByteBuffer encodePayload() {
        org.apache.qpid.protonj2.test.driver.codec.Codec codec =
            org.apache.qpid.protonj2.test.driver.codec.Codec.Factory.create();

        try (ByteArrayOutputStream baOS = new ByteArrayOutputStream(128);
             DataOutputStream output = new DataOutputStream(baOS)) {

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

            codec.encode(output);

            final byte[] encodedBytes = baOS.toByteArray();

            return ByteBuffer.wrap(encodedBytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected abstract class SectionBuilder {

        public TransferInjectAction also() {
            return TransferInjectAction.this;
        }

        public TransferInjectAction and() {
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

        public ApplicationPropertiesBuilder withProperty(String key, Object value) {
            getOrCreateApplicationProperties().setApplicationProperty(key, value);
            return this;
        }

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

        public BodySectionBuilder withValue(String body) {
            TransferInjectAction.this.body = new AmqpValue(body);
            return this;
        }

        public BodySectionBuilder withValue(Map<?, ?> body) {
            TransferInjectAction.this.body = new AmqpValue(body);
            return this;
        }

        public BodySectionBuilder withValue(Object body) {
            TransferInjectAction.this.body = new AmqpValue(body);
            return this;
        }

        public BodySectionBuilder withValue(byte[] body) {
            TransferInjectAction.this.body = new AmqpValue(body == null ? null : new Binary(body));
            return this;
        }

        public BodySectionBuilder withValue(Binary body) {
            TransferInjectAction.this.body = new Data(body);
            return this;
        }

        public BodySectionBuilder withData(byte[] body) {
            TransferInjectAction.this.body = new Data(body == null ? null : new Binary(body));
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

        public BodySectionBuilder withDescribed(DescribedType described) {
            TransferInjectAction.this.body = new AmqpValue(described);
            return this;
        }
    }

    public final class FooterBuilder extends SectionBuilder {

        public FooterBuilder withFooter(String key, Object value) {
            getOrCreateFooter().setFooterProperty(Symbol.valueOf(key), value);
            return this;
        }

        public FooterBuilder withFooter(Symbol key, Object value) {
            getOrCreateFooter().setFooterProperty(key, value);
            return this;
        }
    }

    public final class DeliveryStateBuilder {

        public TransferInjectAction accepted() {
            withState(Accepted.getInstance());
            return TransferInjectAction.this;
        }

        public TransferInjectAction released() {
            withState(Released.getInstance());
            return TransferInjectAction.this;
        }

        public TransferInjectAction rejected() {
            withState(new Rejected());
            return TransferInjectAction.this;
        }

        public TransferInjectAction rejected(String condition, String description) {
            withState(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description)));
            return TransferInjectAction.this;
        }

        public TransferInjectAction rejected(Symbol condition, String description) {
            withState(new Rejected().setError(new ErrorCondition(condition, description)));
            return TransferInjectAction.this;
        }

        public TransferInjectAction rejected(String condition, String description, Map<String, Object> info) {
            withState(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info))));
            return TransferInjectAction.this;
        }

        public TransferInjectAction rejected(Symbol condition, String description, Map<Symbol, Object> info) {
            withState(new Rejected().setError(new ErrorCondition(condition, description, info)));
            return TransferInjectAction.this;
        }

        public TransferInjectAction modified() {
            withState(new Modified());
            return TransferInjectAction.this;
        }

        public TransferInjectAction modified(boolean failed) {
            withState(new Modified().setDeliveryFailed(failed));
            return TransferInjectAction.this;
        }

        public TransferInjectAction modified(boolean failed, boolean undeliverableHere) {
            withState(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverableHere));
            return TransferInjectAction.this;
        }

        public TransferInjectAction modified(boolean failed, boolean undeliverableHere, Map<String, Object> annotations) {
            withState(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverableHere).setMessageAnnotations(TypeMapper.toSymbolKeyedMap(annotations)));
            return TransferInjectAction.this;
        }

        public TransactionalStateBuilder transactional() {
            TransactionalStateBuilder builder = new TransactionalStateBuilder(TransferInjectAction.this);
            withState(builder.getState());
            return builder;
        }
    }

    //----- Provide a complex builder for Transactional DeliveryState

    public static class TransactionalStateBuilder {

        private final TransferInjectAction action;
        private final TransactionalState state = new TransactionalState();

        public TransactionalStateBuilder(TransferInjectAction action) {
            this.action = action;
        }

        public TransactionalState getState() {
            return state;
        }

        public TransferInjectAction also() {
            return action;
        }

        public TransferInjectAction and() {
            return action;
        }

        public TransactionalStateBuilder withTxnId(byte[] txnId) {
            state.setTxnId(new Binary(txnId));
            return this;
        }

        public TransactionalStateBuilder withTxnId(Binary txnId) {
            state.setTxnId(txnId);
            return this;
        }

        public TransactionalStateBuilder withOutcome(DeliveryState outcome) {
            state.setOutcome(outcome);
            return this;
        }

        // ----- Add a layer to allow configuring the outcome without specific type dependencies

        public TransactionalStateBuilder withAccepted() {
            withOutcome(Accepted.getInstance());
            return this;
        }

        public TransactionalStateBuilder withReleased() {
            withOutcome(Released.getInstance());
            return this;
        }

        public TransactionalStateBuilder withRejected() {
            withOutcome(new Rejected());
            return this;
        }

        public TransactionalStateBuilder withRejected(String condition, String description) {
            withOutcome(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description)));
            return this;
        }

        public TransactionalStateBuilder withRejected(Symbol condition, String description) {
            withOutcome(new Rejected().setError(new ErrorCondition(condition, description)));
            return this;
        }

        public TransactionalStateBuilder withRejected(String condition, String description, Map<String, Object> info) {
            withOutcome(new Rejected().setError(new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info))));
            return this;
        }

        public TransactionalStateBuilder withRejected(Symbol condition, String description, Map<Symbol, Object> info) {
            withOutcome(new Rejected().setError(new ErrorCondition(condition, description, info)));
            return this;
        }

        public TransactionalStateBuilder withModified() {
            withOutcome(new Modified());
            return this;
        }

        public TransactionalStateBuilder withModified(boolean failed) {
            withOutcome(new Modified().setDeliveryFailed(failed));
            return this;
        }

        public TransactionalStateBuilder withModified(boolean failed, boolean undeliverableHere) {
            withOutcome(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverableHere));
            return this;
        }

        public TransactionalStateBuilder modified(boolean failed, boolean undeliverableHere, Map<String, Object> annotations) {
            withOutcome(new Modified().setDeliveryFailed(failed).setUndeliverableHere(undeliverableHere).setMessageAnnotations(TypeMapper.toSymbolKeyedMap(annotations)));
            return this;
        }
    }

    public final class MessageBuilder extends SectionBuilder {

        public MessageBuilder withMessageFormat(int format) {
            TransferInjectAction.this.withMessageFormat(format);
            return this;
        }

        public HeaderBuilder withHeader() {
            return TransferInjectAction.this.withHeader();
        }

        public DeliveryAnnotationsBuilder withDeliveryAnnotations() {
            return TransferInjectAction.this.withDeliveryAnnotations();
        }

        public MessageAnnotationsBuilder withMessageAnnotations() {
            return TransferInjectAction.this.withMessageAnnotations();
        }

        public PropertiesBuilder withProperties() {
            return TransferInjectAction.this.withProperties();
        }

        public ApplicationPropertiesBuilder withApplicationProperties() {
            return TransferInjectAction.this.withApplicationProperties();
        }

        public BodySectionBuilder withBody() {
            return TransferInjectAction.this.withBody();
        }

        public FooterBuilder withFooter() {
            return TransferInjectAction.this.withFooter();
        }
    }

    private static byte[] generateUniqueDeliveryTag() {
        final byte[] tag = new byte[Long.BYTES + Long.BYTES];
        final UUID uuid = UUID.randomUUID();

        writeLong(uuid.getMostSignificantBits(), tag, 0);
        writeLong(uuid.getLeastSignificantBits(), tag, Long.BYTES);

        return tag;
    }

    private static byte[] writeLong(long value, byte[] destination, int offset) {
        destination[offset++] = (byte) (value >>> 56);
        destination[offset++] = (byte) (value >>> 48);
        destination[offset++] = (byte) (value >>> 40);
        destination[offset++] = (byte) (value >>> 32);
        destination[offset++] = (byte) (value >>> 24);
        destination[offset++] = (byte) (value >>> 16);
        destination[offset++] = (byte) (value >>> 8);
        destination[offset++] = (byte) (value >>> 0);

        return destination;
    }
}
