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

package org.apache.qpid.protonj2.test.driver.matchers.transport;

import static org.hamcrest.CoreMatchers.equalTo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedByte;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;
import org.apache.qpid.protonj2.test.driver.expectations.TransferExpectation;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpSequenceMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAnyBodySectionMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedBodySectionMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedDataMatcher;
import org.apache.qpid.protonj2.test.driver.util.StringUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher used by a {@link TransferExpectation} to build a matcher for the message
 * payload that accompanies the {@link Transfer}. The matcher generally adheres to
 * the standard AMQP message format zero layout.
 */
public class TransferMessageMatcher extends TypeSafeMatcher<ByteBuffer> {

    private final TransferExpectation expectation;

    private HeaderMatcher headersMatcher;
    private DeliveryAnnotationsMatcher deliveryAnnotationsMatcher;
    private MessageAnnotationsMatcher messageAnnotationsMatcher;
    private PropertiesMatcher propertiesMatcher;
    private ApplicationPropertiesMatcher applicationPropertiesMatcher;
    private List<EncodedBodySectionMatcher> bodySectionMatchers = new ArrayList<>();
    private FooterMatcher footersMatcher;

    // String buckets for mismatch error descriptions.
    private String headerMatcherFailureDescription;
    private String deliveryAnnotationsMatcherFailureDescription;
    private String messageAnnotationsMatcherFailureDescription;
    private String propertiesMatcherFailureDescription;
    private String applicationPropertiesMatcherFailureDescription;
    private String msgContentMatcherFailureDescription;
    private String footerMatcherFailureDescription;

    public TransferMessageMatcher(TransferExpectation expectation) {
        this.expectation = expectation;
    }

    public TransferExpectation also() {
        return expectation;
    }

    public TransferExpectation and() {
        return expectation;
    }

    @Override
    protected boolean matchesSafely(ByteBuffer receivedBinary) {
        final ByteBuffer receivedSlice = receivedBinary.slice().asReadOnlyBuffer();

        int bytesConsumed = 0;

        // MessageHeader Section
        if (headersMatcher != null) {
            try {
                bytesConsumed += headersMatcher.getInnerMatcher().verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                headerMatcherFailureDescription = "\nBuffer of bytes passed to Header Matcher: " + receivedSlice;
                headerMatcherFailureDescription += "\nActual encoded form of remaining bytes passed: " + StringUtils.toQuotedString(receivedSlice);
                headerMatcherFailureDescription += "\nHeader Matcher generated throwable: " + t.getMessage();

                return false;
            }
        }

        // DeliveryAnnotations Section
        if (deliveryAnnotationsMatcher != null) {
            try {
                bytesConsumed += deliveryAnnotationsMatcher.getInnerMatcher().verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                deliveryAnnotationsMatcherFailureDescription = "\nBuffer of bytes passed to Delivery Annotations Matcher: " + receivedSlice;
                deliveryAnnotationsMatcherFailureDescription += "\nActual encoded form of remaining bytes passed: " + StringUtils.toQuotedString(receivedSlice);
                deliveryAnnotationsMatcherFailureDescription += "\nDelivery Annotations Matcher generated throwable: " + t.getMessage();

                return false;
            }
        }

        // MessageAnnotations Section
        if (messageAnnotationsMatcher != null) {
            try {
                bytesConsumed += messageAnnotationsMatcher.getInnerMatcher().verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                messageAnnotationsMatcherFailureDescription = "\nBuffer of bytes passed to Message Annotations Matcher: " + receivedSlice;
                messageAnnotationsMatcherFailureDescription += "\nActual encoded form of remaining bytes passed: " + StringUtils.toQuotedString(receivedSlice);
                messageAnnotationsMatcherFailureDescription += "\nMessage Annotations Matcher generated throwable: " + t.getMessage();

                return false;
            }
        }

        // Properties Section
        if (propertiesMatcher != null) {
            try {
                bytesConsumed += propertiesMatcher.getInnerMatcher().verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                propertiesMatcherFailureDescription = "\nBuffer of bytes passed to Properties Matcher: " + receivedSlice;
                propertiesMatcherFailureDescription += "\nActual encoded form of remaining bytes passed: " + StringUtils.toQuotedString(receivedSlice);
                propertiesMatcherFailureDescription += "\nProperties Matcher generated throwable: " + t.getMessage();

                return false;
            }
        }

        // Application Properties Section
        if (applicationPropertiesMatcher != null) {
            try {
                bytesConsumed += applicationPropertiesMatcher.getInnerMatcher().verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                applicationPropertiesMatcherFailureDescription = "\nBuffer of bytes passed to Application Properties Matcher: " + receivedSlice;
                applicationPropertiesMatcherFailureDescription += "\nActual encoded form of remaining bytes passed: " + StringUtils.toQuotedString(receivedSlice);
                applicationPropertiesMatcherFailureDescription += "\nApplication Properties Matcher generated throwable: " + t.getMessage();

                return false;
            }
        }

        // Message Content Body Section, already a Matcher<Binary>
        if (!bodySectionMatchers.isEmpty()) {
            final ByteBuffer slicedMsgContext = receivedSlice.slice();

            for (Matcher<ByteBuffer> msgContentMatcher : bodySectionMatchers) {
                final int originalReadableBytes = slicedMsgContext.remaining();
                final boolean contentMatches = msgContentMatcher.matches(slicedMsgContext);
                if (!contentMatches) {
                    Description desc = new StringDescription();
                    msgContentMatcher.describeTo(desc);
                    msgContentMatcher.describeMismatch(slicedMsgContext, desc);

                    msgContentMatcherFailureDescription = "\nBuffer of bytes passed to message contents Matcher: " + slicedMsgContext;
                    msgContentMatcherFailureDescription += "\nActual encoded form of remaining bytes passed: " + StringUtils.toQuotedString(receivedSlice);
                    msgContentMatcherFailureDescription += "\nMessageContentMatcher mismatch Description:";
                    msgContentMatcherFailureDescription += desc.toString();

                    return false;
                }

                bytesConsumed += originalReadableBytes - slicedMsgContext.remaining();
                receivedSlice.position(bytesConsumed);
            }
        }

        // Footers Section
        if (footersMatcher != null) {
            try {
                bytesConsumed += footersMatcher.getInnerMatcher().verify(receivedSlice.slice());
            } catch (Throwable t) {
                footerMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to " +
                                                  "FooterMatcher: " + receivedSlice;
                footerMatcherFailureDescription += "\nFooterMatcher generated throwable: " + t.getMessage();

                return false;
            }
        }

        return true;
    }

    public TransferMessageMatcher withMessageFormat(int format) {
        this.expectation.withMessageFormat(format);
        return this;
    }

    public HeaderMatcher withHeader() {
        if (headersMatcher == null) {
            headersMatcher = new HeaderMatcher(this);
        }

        if (deliveryAnnotationsMatcher != null || messageAnnotationsMatcher != null ||
            propertiesMatcher != null || applicationPropertiesMatcher != null ||
            !bodySectionMatchers.isEmpty() || footersMatcher != null) {

            headersMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        } else {
            headersMatcher.getInnerMatcher().setAllowTrailingBytes(false);
        }

        return headersMatcher;
    }

    public DeliveryAnnotationsMatcher withDeliveryAnnotations() {
        if (deliveryAnnotationsMatcher == null) {
            deliveryAnnotationsMatcher = new DeliveryAnnotationsMatcher(this);
        }

        if (headersMatcher != null) {
            headersMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }

        if (messageAnnotationsMatcher != null || propertiesMatcher != null || applicationPropertiesMatcher != null ||
            !bodySectionMatchers.isEmpty() || footersMatcher != null) {

            deliveryAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        } else {
            deliveryAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(false);
        }

        return deliveryAnnotationsMatcher;
    }

    public MessageAnnotationsMatcher withMessageAnnotations() {
        if (messageAnnotationsMatcher == null) {
            messageAnnotationsMatcher = new MessageAnnotationsMatcher(this);
        }

        if (headersMatcher != null) {
            headersMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (deliveryAnnotationsMatcher != null) {
            deliveryAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }

        if (propertiesMatcher != null || applicationPropertiesMatcher != null ||
            !bodySectionMatchers.isEmpty() || footersMatcher != null) {

            messageAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        } else {
            messageAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(false);
        }

        return messageAnnotationsMatcher;
    }

    public PropertiesMatcher withProperties() {
        if (propertiesMatcher == null) {
            propertiesMatcher = new PropertiesMatcher(this);
        }

        if (headersMatcher != null) {
            headersMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (deliveryAnnotationsMatcher != null) {
            deliveryAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (messageAnnotationsMatcher != null) {
            messageAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }

        if (applicationPropertiesMatcher != null || !bodySectionMatchers.isEmpty() || footersMatcher != null) {
            propertiesMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        } else {
            propertiesMatcher.getInnerMatcher().setAllowTrailingBytes(false);
        }

        return propertiesMatcher;
    }

    public ApplicationPropertiesMatcher withApplicationProperties() {
        if (applicationPropertiesMatcher == null) {
            applicationPropertiesMatcher = new ApplicationPropertiesMatcher(this);
        }

        if (headersMatcher != null) {
            headersMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (deliveryAnnotationsMatcher != null) {
            deliveryAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (messageAnnotationsMatcher != null) {
            messageAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (propertiesMatcher != null) {
            propertiesMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }

        if (!bodySectionMatchers.isEmpty() || footersMatcher != null) {
            applicationPropertiesMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        } else {
            applicationPropertiesMatcher.getInnerMatcher().setAllowTrailingBytes(false);
        }

        return applicationPropertiesMatcher;
    }

    public TransferMessageMatcher withSequence(List<?> sequence) {
        return withBodySection(new EncodedAmqpSequenceMatcher(sequence, footersMatcher != null));
    }

    public TransferMessageMatcher withSequence(Matcher<?> sequenceMatcher) {
        return withBodySection(new EncodedAmqpSequenceMatcher(sequenceMatcher, footersMatcher != null));
    }

    public TransferMessageMatcher withData(byte[] payload) {
        return withBodySection(new EncodedDataMatcher(payload, footersMatcher != null));
    }

    public TransferMessageMatcher withData(Matcher<?> payloadMatcher) {
        return withBodySection(new EncodedDataMatcher(payloadMatcher, footersMatcher != null));
    }

    public TransferMessageMatcher withValue(Object value) {
        return withBodySection(new EncodedAmqpValueMatcher(value, footersMatcher != null));
    }

    public TransferMessageMatcher withValue(Matcher<?> valueMatcher) {
        return withBodySection(new EncodedAmqpValueMatcher(valueMatcher, footersMatcher != null));
    }

    public TransferMessageMatcher withValidBodySection() {
        return withBodySection(new EncodedAnyBodySectionMatcher(footersMatcher != null));
    }

    protected TransferMessageMatcher withBodySection(EncodedBodySectionMatcher matcher) {
        Objects.requireNonNull(matcher, "Body section matcher cannot be null");

        if (headersMatcher != null) {
            headersMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (deliveryAnnotationsMatcher != null) {
            deliveryAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (messageAnnotationsMatcher != null) {
            messageAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (propertiesMatcher != null) {
            propertiesMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (applicationPropertiesMatcher != null) {
            applicationPropertiesMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }

        if (!bodySectionMatchers.isEmpty()) {
            bodySectionMatchers.get(bodySectionMatchers.size() - 1).setAllowTrailingBytes(true);
        }

        bodySectionMatchers.add(matcher);

        return this;
    }

    public FooterMatcher withFooters() {
        if (footersMatcher == null) {
            footersMatcher = new FooterMatcher(this);
        }

        if (headersMatcher != null) {
            headersMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (deliveryAnnotationsMatcher != null) {
            deliveryAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (messageAnnotationsMatcher != null) {
            messageAnnotationsMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (propertiesMatcher != null) {
            propertiesMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }
        if (applicationPropertiesMatcher != null) {
            applicationPropertiesMatcher.getInnerMatcher().setAllowTrailingBytes(true);
        }

        if (!bodySectionMatchers.isEmpty()) {
            bodySectionMatchers.get(bodySectionMatchers.size() - 1).setAllowTrailingBytes(true);
        }

        return footersMatcher;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a Binary encoding of a Transfer frames payload, containing an AMQP message");
    }

    @Override
    protected void describeMismatchSafely(ByteBuffer item, Description mismatchDescription) {
        mismatchDescription.appendText("\nActual encoded form of the full Transfer frame payload: ").appendValue(item);

        // MessageHeaders Section
        if (headerMatcherFailureDescription != null) {
            mismatchDescription.appendText("\nMessageHeadersMatcherFailed!");
            mismatchDescription.appendText(headerMatcherFailureDescription);
            return;
        }

        // MessageHeaders Section
        if (deliveryAnnotationsMatcherFailureDescription != null) {
            mismatchDescription.appendText("\nDeliveryAnnotationsMatcherFailed!");
            mismatchDescription.appendText(deliveryAnnotationsMatcherFailureDescription);
            return;
        }

        // MessageAnnotations Section
        if (messageAnnotationsMatcherFailureDescription != null) {
            mismatchDescription.appendText("\nMessageAnnotationsMatcherFailed!");
            mismatchDescription.appendText(messageAnnotationsMatcherFailureDescription);
            return;
        }

        // Properties Section
        if (propertiesMatcherFailureDescription != null) {
            mismatchDescription.appendText("\nPropertiesMatcherFailed!");
            mismatchDescription.appendText(propertiesMatcherFailureDescription);
            return;
        }

        // Application Properties Section
        if (applicationPropertiesMatcherFailureDescription != null) {
            mismatchDescription.appendText("\nApplicationPropertiesMatcherFailed!");
            mismatchDescription.appendText(applicationPropertiesMatcherFailureDescription);
            return;
        }

        // Message Content Body Section
        if (msgContentMatcherFailureDescription != null) {
            mismatchDescription.appendText("\nContentMatcherFailed!");
            mismatchDescription.appendText(msgContentMatcherFailureDescription);
            return;
        }

        // Footer Section
        if (footerMatcherFailureDescription != null) {
            mismatchDescription.appendText("\nContentMatcherFailed!");
            mismatchDescription.appendText(footerMatcherFailureDescription);
        }
    }

    public static final class HeaderMatcher {

        private final org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher matcher =
            new org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher(false);

        private final TransferMessageMatcher transferMatcher;

        public HeaderMatcher(TransferMessageMatcher transferMatcher) {
            this.transferMatcher = transferMatcher;
        }

        public TransferMessageMatcher also() {
            return transferMatcher;
        }

        public TransferMessageMatcher and() {
            return transferMatcher;
        }

        public HeaderMatcher withDurability(boolean durable) {
            matcher.withDurable(equalTo(durable));
            return this;
        }

        public HeaderMatcher withDurability(Boolean durable) {
            matcher.withDurable(equalTo(durable));
            return this;
        }

        public HeaderMatcher withPriority(byte priority) {
            matcher.withPriority(equalTo(UnsignedByte.valueOf(priority)));
            return this;
        }

        public HeaderMatcher withPriority(UnsignedByte priority) {
            matcher.withPriority(equalTo(priority));
            return this;
        }

        public HeaderMatcher withTimeToLive(int timeToLive) {
            matcher.withTtl(equalTo(UnsignedInteger.valueOf(timeToLive)));
            return this;
        }

        public HeaderMatcher withTimeToLive(long timeToLive) {
            matcher.withTtl(equalTo(UnsignedInteger.valueOf(timeToLive)));
            return this;
        }

        public HeaderMatcher withTimeToLive(UnsignedInteger timeToLive) {
            matcher.withTtl(equalTo(timeToLive));
            return this;
        }

        public HeaderMatcher withFirstAcquirer(boolean durable) {
            matcher.withFirstAcquirer(equalTo(durable));
            return this;
        }

        public HeaderMatcher withFirstAcquirer(Boolean durable) {
            matcher.withFirstAcquirer(equalTo(durable));
            return this;
        }

        public HeaderMatcher withDeliveryCount(int deliveryCount) {
            matcher.withDeliveryCount(equalTo(UnsignedInteger.valueOf(deliveryCount)));
            return this;
        }

        public HeaderMatcher withDeliveryCount(long deliveryCount) {
            matcher.withDeliveryCount(equalTo(UnsignedInteger.valueOf(deliveryCount)));
            return this;
        }

        public HeaderMatcher withDeliveryCount(UnsignedInteger deliveryCount) {
            matcher.withDeliveryCount(equalTo(deliveryCount));
            return this;
        }

        org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher getInnerMatcher() {
            return matcher;
        }
    }

    public static final class DeliveryAnnotationsMatcher {

        private final org.apache.qpid.protonj2.test.driver.matchers.messaging.DeliveryAnnotationsMatcher matcher =
            new org.apache.qpid.protonj2.test.driver.matchers.messaging.DeliveryAnnotationsMatcher(false);

        private final TransferMessageMatcher transferMatcher;

        public DeliveryAnnotationsMatcher(TransferMessageMatcher transferMatcher) {
            this.transferMatcher = transferMatcher;
        }

        public TransferMessageMatcher also() {
            return transferMatcher;
        }

        public TransferMessageMatcher and() {
            return transferMatcher;
        }

        public DeliveryAnnotationsMatcher withAnnotation(String key, Object value) {
            matcher.withEntry(Symbol.valueOf(key), value);
            return this;
        }

        public DeliveryAnnotationsMatcher withAnnotation(Symbol key, Object value) {
            matcher.withEntry(key, value);
            return this;
        }

        org.apache.qpid.protonj2.test.driver.matchers.messaging.DeliveryAnnotationsMatcher getInnerMatcher() {
            return matcher;
        }
    }

    public static final class MessageAnnotationsMatcher {

        private final org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher matcher =
            new org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher(false);

        private final TransferMessageMatcher transferMatcher;

        public MessageAnnotationsMatcher(TransferMessageMatcher transferMatcher) {
            this.transferMatcher = transferMatcher;
        }

        public TransferMessageMatcher also() {
            return transferMatcher;
        }

        public TransferMessageMatcher and() {
            return transferMatcher;
        }

        public MessageAnnotationsMatcher withAnnotation(String key, Object value) {
            matcher.withEntry(Symbol.valueOf(key), value);
            return this;
        }

        public MessageAnnotationsMatcher withAnnotation(Symbol key, Object value) {
            matcher.withEntry(key, value);
            return this;
        }

        org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher getInnerMatcher() {
            return matcher;
        }
    }

    public static final class PropertiesMatcher {

        private final org.apache.qpid.protonj2.test.driver.matchers.messaging.PropertiesMatcher matcher =
            new org.apache.qpid.protonj2.test.driver.matchers.messaging.PropertiesMatcher(false);

        private final TransferMessageMatcher transferMatcher;

        public PropertiesMatcher(TransferMessageMatcher transferMatcher) {
            this.transferMatcher = transferMatcher;
        }

        public TransferMessageMatcher also() {
            return transferMatcher;
        }

        public TransferMessageMatcher and() {
            return transferMatcher;
        }

        public PropertiesMatcher withMessageId(Object messageId) {
            matcher.withMessageId(messageId);
            return this;
        }

        public PropertiesMatcher withUserId(byte[] userId) {
            matcher.withUserId(userId);
            return this;
        }

        public PropertiesMatcher withUserId(Binary userId) {
            matcher.withUserId(userId);
            return this;
        }

        public PropertiesMatcher withTo(String to) {
            matcher.withTo(to);
            return this;
        }

        public PropertiesMatcher withSubject(String subject) {
            matcher.withSubject(subject);
            return this;
        }

        public PropertiesMatcher withReplyTo(String replyTo) {
            matcher.withReplyTo(replyTo);
            return this;
        }

        public PropertiesMatcher withCorrelationId(Object correlationId) {
            matcher.withCorrelationId(correlationId);
            return this;
        }

        public PropertiesMatcher withContentType(String contentType) {
            matcher.withContentType(contentType);
            return this;
        }

        public PropertiesMatcher withContentType(Symbol contentType) {
            matcher.withContentType(contentType);
            return this;
        }

        public PropertiesMatcher withContentEncoding(String contentEncoding) {
            matcher.withContentEncoding(contentEncoding);
            return this;
        }

        public PropertiesMatcher withContentEncoding(Symbol contentEncoding) {
            matcher.withContentEncoding(contentEncoding);
            return this;
        }

        public PropertiesMatcher withAbsoluteExpiryTime(int absoluteExpiryTime) {
            matcher.withAbsoluteExpiryTime(absoluteExpiryTime);
            return this;
        }

        public PropertiesMatcher withAbsoluteExpiryTime(long absoluteExpiryTime) {
            matcher.withAbsoluteExpiryTime(absoluteExpiryTime);
            return this;
        }

        public PropertiesMatcher withAbsoluteExpiryTime(Long absoluteExpiryTime) {
            matcher.withAbsoluteExpiryTime(absoluteExpiryTime);
            return this;
        }

        public PropertiesMatcher withCreationTime(int creationTime) {
            matcher.withCreationTime(creationTime);
            return this;
        }

        public PropertiesMatcher withCreationTime(long creationTime) {
            matcher.withCreationTime(creationTime);
            return this;
        }

        public PropertiesMatcher withCreationTime(Long creationTime) {
            matcher.withCreationTime(creationTime);
            return this;
        }

        public PropertiesMatcher withGroupId(String groupId) {
            matcher.withGroupId(groupId);
            return this;
        }

        public PropertiesMatcher withGroupSequence(int groupSequence) {
            matcher.withGroupSequence(groupSequence);
            return this;
        }

        public PropertiesMatcher withGroupSequence(long groupSequence) {
            matcher.withGroupSequence(groupSequence);
            return this;
        }

        public PropertiesMatcher withGroupSequence(Long groupSequence) {
            matcher.withGroupSequence(groupSequence);
            return this;
        }

        public PropertiesMatcher withReplyToGroupId(String replyToGroupId) {
            matcher.withReplyToGroupId(replyToGroupId);
            return this;
        }

        org.apache.qpid.protonj2.test.driver.matchers.messaging.PropertiesMatcher getInnerMatcher() {
            return matcher;
        }
    }

    public static final class ApplicationPropertiesMatcher {

        private final org.apache.qpid.protonj2.test.driver.matchers.messaging.ApplicationPropertiesMatcher matcher =
            new org.apache.qpid.protonj2.test.driver.matchers.messaging.ApplicationPropertiesMatcher(false);

        private final TransferMessageMatcher transferMatcher;

        public ApplicationPropertiesMatcher(TransferMessageMatcher transferMatcher) {
            this.transferMatcher = transferMatcher;
        }

        public TransferMessageMatcher also() {
            return transferMatcher;
        }

        public TransferMessageMatcher and() {
            return transferMatcher;
        }

        public ApplicationPropertiesMatcher withProperty(String key, Object value) {
            matcher.withEntry(key, value);
            return this;
        }

        org.apache.qpid.protonj2.test.driver.matchers.messaging.ApplicationPropertiesMatcher getInnerMatcher() {
            return matcher;
        }
    }

    public static final class FooterMatcher {

        private final org.apache.qpid.protonj2.test.driver.matchers.messaging.FooterMatcher matcher =
            new org.apache.qpid.protonj2.test.driver.matchers.messaging.FooterMatcher(false);

        private final TransferMessageMatcher transferMatcher;

        public FooterMatcher(TransferMessageMatcher transferMatcher) {
            this.transferMatcher = transferMatcher;
        }

        public TransferMessageMatcher also() {
            return transferMatcher;
        }

        public TransferMessageMatcher and() {
            return transferMatcher;
        }

        public FooterMatcher withFooter(String key, Object value) {
            matcher.withEntry(Symbol.valueOf(key), value);
            return this;
        }

        public FooterMatcher withFooter(Symbol key, Object value) {
            matcher.withEntry(key, value);
            return this;
        }

        org.apache.qpid.protonj2.test.driver.matchers.messaging.FooterMatcher getInnerMatcher() {
            return matcher;
        }
    }
}
