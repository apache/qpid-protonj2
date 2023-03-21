/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.qpid.protonj2.test.driver.matchers.transport;

import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.qpid.protonj2.test.driver.matchers.messaging.ApplicationPropertiesMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.DeliveryAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.FooterMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.HeaderMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.MessageAnnotationsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.messaging.PropertiesMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeMatcher;

/**
 * Used to verify the Transfer frame payload, i.e the sections of the AMQP
 * message such as the header, properties, and body sections.
 */
public class TransferPayloadCompositeMatcher extends TypeSafeMatcher<ByteBuffer> {

    private HeaderMatcher headersMatcher;
    private String headerMatcherFailureDescription;
    private DeliveryAnnotationsMatcher deliveryAnnotationsMatcher;
    private String deliveryAnnotationsMatcherFailureDescription;
    private MessageAnnotationsMatcher messageAnnotationsMatcher;
    private String messageAnnotationsMatcherFailureDescription;
    private PropertiesMatcher propertiesMatcher;
    private String propertiesMatcherFailureDescription;
    private ApplicationPropertiesMatcher applicationPropertiesMatcher;
    private String applicationPropertiesMatcherFailureDescription;
    private List<Matcher<ByteBuffer>> msgContentMatchers = new ArrayList<>();
    private String msgContentMatcherFailureDescription;
    private FooterMatcher footersMatcher;
    private String footerMatcherFailureDescription;
    private Matcher<Integer> payloadLengthMatcher;
    private String payloadLengthMatcherFailureDescription;

    public TransferPayloadCompositeMatcher() {
    }

    @Override
    protected boolean matchesSafely(final ByteBuffer receivedBinary) {
        final ByteBuffer receivedSlice = receivedBinary.slice().asReadOnlyBuffer();
        final int origLength = receivedBinary.remaining();

        int bytesConsumed = 0;

        // Length Matcher
        if (payloadLengthMatcher != null) {
            try {
                assertThat("Payload length should match", origLength, payloadLengthMatcher);
            } catch (Throwable t) {
                payloadLengthMatcherFailureDescription = "\nPayload Length Matcher generated throwable: " + t;

                return false;
            }
        }

        // MessageHeader Section
        if (headersMatcher != null) {
            try {
                bytesConsumed += headersMatcher.verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                headerMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to MessageHeaderMatcher: " + receivedSlice;
                headerMatcherFailureDescription += "\nMessageHeaderMatcher generated throwable: " + t;

                return false;
            }
        }

        // DeliveryAnnotations Section
        if (deliveryAnnotationsMatcher != null) {
            try {
                bytesConsumed += deliveryAnnotationsMatcher.verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                deliveryAnnotationsMatcherFailureDescription = "\nActual encoded form of remaining bytes passed " +
                                                               "to DeliveryAnnotationsMatcher: " + receivedSlice;
                deliveryAnnotationsMatcherFailureDescription += "\nDeliveryAnnotationsMatcher generated throwable: " + t;

                return false;
            }
        }

        // MessageAnnotations Section
        if (messageAnnotationsMatcher != null) {
            try {
                bytesConsumed += messageAnnotationsMatcher.verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                messageAnnotationsMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to " +
                                                              "MessageAnnotationsMatcher: " + receivedSlice;
                messageAnnotationsMatcherFailureDescription += "\nMessageAnnotationsMatcher generated throwable: " + t;

                return false;
            }
        }

        // Properties Section
        if (propertiesMatcher != null) {
            try {
                bytesConsumed += propertiesMatcher.verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                propertiesMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to " +
                                                      "PropertiesMatcher: " + receivedSlice;
                propertiesMatcherFailureDescription += "\nPropertiesMatcher generated throwable: " + t;

                return false;
            }
        }

        // Application Properties Section
        if (applicationPropertiesMatcher != null) {
            try {
                bytesConsumed += applicationPropertiesMatcher.verify(receivedSlice.slice());
                receivedSlice.position(bytesConsumed);
            } catch (Throwable t) {
                applicationPropertiesMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to " +
                                                                 "ApplicationPropertiesMatcher: " + receivedSlice;
                applicationPropertiesMatcherFailureDescription += "\nApplicationPropertiesMatcher generated throwable: " + t;

                return false;
            }
        }

        // Message Content Body Section, already a Matcher<Binary>
        if (!msgContentMatchers.isEmpty()) {
            final ByteBuffer slicedMsgContext = receivedSlice.slice();

            for (Matcher<ByteBuffer> msgContentMatcher : msgContentMatchers) {
                final int originalReadableBytes = slicedMsgContext.remaining();
                final boolean contentMatches = msgContentMatcher.matches(slicedMsgContext);
                if (!contentMatches) {
                    Description desc = new StringDescription();
                    msgContentMatcher.describeTo(desc);
                    msgContentMatcher.describeMismatch(receivedSlice, desc);

                    msgContentMatcherFailureDescription = "\nMessageContentMatcher mismatch Description:";
                    msgContentMatcherFailureDescription += desc.toString();

                    return false;
                }

                bytesConsumed += originalReadableBytes - slicedMsgContext.remaining();
                receivedSlice.position(bytesConsumed);
            }
        }

        // MessageAnnotations Section
        if (footersMatcher != null) {
            try {
                bytesConsumed += footersMatcher.verify(receivedSlice.slice());
            } catch (Throwable t) {
                footerMatcherFailureDescription = "\nActual encoded form of remaining bytes passed to " +
                                                  "FooterMatcher: " + receivedSlice;
                footerMatcherFailureDescription += "\nFooterMatcher generated throwable: " + t;

                return false;
            }
        }

        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a Binary encoding of a Transfer frames payload, containing an AMQP message");
    }

    @Override
    protected void describeMismatchSafely(ByteBuffer item, Description mismatchDescription) {
        mismatchDescription.appendText("\nActual encoded form of the full Transfer frame payload: ").appendValue(item);

        // Payload Length
        if (payloadLengthMatcherFailureDescription != null) {
            mismatchDescription.appendText("\nPayloadLengthMatcherFailed!");
            mismatchDescription.appendText(payloadLengthMatcherFailureDescription);
            return;
        }

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

    public void setHeadersMatcher(HeaderMatcher headersMatcher) {
        this.headersMatcher = headersMatcher;
    }

    public void setDeliveryAnnotationsMatcher(DeliveryAnnotationsMatcher deliveryAnnotationsMatcher) {
        this.deliveryAnnotationsMatcher = deliveryAnnotationsMatcher;
    }

    public void setMessageAnnotationsMatcher(MessageAnnotationsMatcher msgAnnotationsMatcher) {
        this.messageAnnotationsMatcher = msgAnnotationsMatcher;
    }

    public void setPropertiesMatcher(PropertiesMatcher propsMatcher) {
        this.propertiesMatcher = propsMatcher;
    }

    public void setApplicationPropertiesMatcher(ApplicationPropertiesMatcher appPropsMatcher) {
        this.applicationPropertiesMatcher = appPropsMatcher;
    }

    public void setMessageContentMatcher(Matcher<ByteBuffer> msgContentMatcher) {
        if (msgContentMatchers.isEmpty()) {
            msgContentMatchers.add(msgContentMatcher);
        } else {
            msgContentMatchers.set(0, msgContentMatcher);
        }
    }

    public void addMessageContentMatcher(Matcher<ByteBuffer> msgContentMatcher) {
        msgContentMatchers.add(msgContentMatcher);
    }

    public void setFootersMatcher(FooterMatcher footersMatcher) {
        this.footersMatcher = footersMatcher;
    }

    public void setPayloadLengthMatcher(Matcher<Integer> payloadLengthMatcher) {
        this.payloadLengthMatcher = payloadLengthMatcher;
    }
}