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
package org.apache.qpid.protonj2.test.driver.expectations;

import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.ScriptedExpectation;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslChallenge;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslInit;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslMechanisms;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslOutcome;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslResponse;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.codec.transport.Attach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Begin;
import org.apache.qpid.protonj2.test.driver.codec.transport.Close;
import org.apache.qpid.protonj2.test.driver.codec.transport.Detach;
import org.apache.qpid.protonj2.test.driver.codec.transport.Disposition;
import org.apache.qpid.protonj2.test.driver.codec.transport.End;
import org.apache.qpid.protonj2.test.driver.codec.transport.Flow;
import org.apache.qpid.protonj2.test.driver.codec.transport.HeartBeat;
import org.apache.qpid.protonj2.test.driver.codec.transport.Open;
import org.apache.qpid.protonj2.test.driver.codec.transport.Transfer;
import org.apache.qpid.protonj2.test.driver.exceptions.UnexpectedPerformativeError;
import org.hamcrest.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base for expectations that need to handle matchers against fields in the
 * value being expected.
 *
 * @param <T> The type being validated
 */
public abstract class AbstractExpectation<T extends ListDescribedType> implements ScriptedExpectation {

    private static final Logger LOG = LoggerFactory.getLogger(AMQPTestDriver.class);

    public static int ANY_CHANNEL = -1;

    protected final AMQPTestDriver driver;

    protected int expectedChannel = ANY_CHANNEL;
    private UnsignedInteger frameSize;
    private boolean optional;

    private Predicate<T> verificationPredicate;
    private Consumer<T> performativeCapture;

    public AbstractExpectation(AMQPTestDriver driver) {
        this.driver = driver;
    }

    //----- Configure base expectations

    public AbstractExpectation<T> onChannel(int channel) {
        this.expectedChannel = channel;
        return this;
    }

    /**
     * @return true if this element represents an optional part of the script.
     */
    @Override
    public boolean isOptional() {
        return optional;
    }

    /**
     * Marks this expectation as optional which can be useful when a frames arrival may or may not
     * occur based on some other timing in the test.
     *
     * @return if the frame expectation is optional and its absence shouldn't fail the test.
     */
    public AbstractExpectation<T> optional() {
        optional = true;
        return this;
    }

    /**
     * Configures the expected frame size that this expectation will enforce when processing
     * a new scripted expectation.
     *
     * @param frameSize
     * 		The expected size of the enclosing frame that carries the incoming data.
     *
     * @return this expectation instance.
     */
    public AbstractExpectation<T> withFrameSize(int frameSize) {
        this.frameSize = new UnsignedInteger(frameSize);
        return this;
    }

    /**
     * Allows a test to supply a {@link Predicate} that will be allowed to further test an
     * incoming performative if all other tests verifications pass. If the predicate call
     * throws any exceptions the verification process is failed.
     *
     * @param predicate
     * 		The user supplied {@link Predicate} that will be called as a last stage verification.
     *
     * @return this expectation instance.
     */
    public AbstractExpectation<T> withPredicate(Predicate<T> predicate) {
        this.verificationPredicate = predicate;
        return this;
    }

    /**
     * Allows the test to provide a capture {@link Consumer} that will be called to allow
     * the test to capture the incoming performative instance for use in testing. Any errors
     * that are thrown from the provided capture are ignored.
     *
     * @param capture
     * 		The user supplied {@link Consumer} that will be provided the performative if verified.
     *
     * @return this expectation instance.
     */
    public AbstractExpectation<T> withCapture(Consumer<T> capture) {
        this.performativeCapture = capture;
        return this;
    }

    //------ Abstract classes use these methods to control validation

    /**
     * Verifies the fields of the performative against any matchers registered.
     *
     * @param performative
     *      the performative received which will be validated against the configured matchers
     *
     * @throws AssertionError if a registered matcher assertion is not met.
     */
    protected final void verifyPerformative(T performative) throws AssertionError {
        LOG.debug("About to check the fields of the performative." +
                  "\n  Received:" + performative + "\n  Expectations: " + getExpectationMatcher());

        assertThat("Performative does not match expectation", performative, getExpectationMatcher());
    }

    protected final void verifyPayload(ByteBuffer payload) {
        if (getPayloadMatcher() != null) {
            assertThat("Payload does not match expectation", payload, getPayloadMatcher());
        } else if (payload != null) {
            throw new AssertionError("Performative should not have been sent with a payload: ");
        }
    }

    protected final void verifyChannel(int channel) {
        if (expectedChannel != ANY_CHANNEL && expectedChannel != channel) {
            throw new AssertionError("Expected send on channel + " + expectedChannel + ": but was on channel:" + channel);
        }
    }

    protected final void verifyFrameSize(int frameSize) {
        if (this.frameSize != null && !this.frameSize.equals(UnsignedInteger.valueOf(frameSize))) {
            throw new AssertionError(String.format(
                "Expected frame size %s did not match that of the received frame: %d", this.frameSize, frameSize));
        }
    }

    protected abstract Matcher<ListDescribedType> getExpectationMatcher();

    protected abstract Class<T> getExpectedTypeClass();

    protected Matcher<ByteBuffer> getPayloadMatcher() {
        return null;
    }

    //----- Base implementation of the handle methods to describe when we get wrong type.

    @Override
    public void handleOpen(int frameSize, Open open, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, open, payload, channel, context);
    }

    @Override
    public void handleBegin(int frameSize, Begin begin, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, begin, payload, channel, context);
    }

    @Override
    public void handleAttach(int frameSize, Attach attach, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, attach, payload, channel, context);
    }

    @Override
    public void handleFlow(int frameSize, Flow flow, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, flow, payload, channel, context);
    }

    @Override
    public void handleTransfer(int frameSize, Transfer transfer, ByteBuffer payload, int channel,AMQPTestDriver context) {
        doVerification(frameSize, transfer, payload, channel, context);
    }

    @Override
    public void handleDisposition(int frameSize, Disposition disposition, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, disposition, payload, channel, context);
    }

    @Override
    public void handleDetach(int frameSize, Detach detach, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, detach, payload, channel, context);
    }

    @Override
    public void handleEnd(int frameSize, End end, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, end, payload, channel, context);
    }

    @Override
    public void handleClose(int frameSize, Close close, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, close, payload, channel, context);
    }

    @Override
    public void handleHeartBeat(int frameSize, HeartBeat thump, ByteBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(frameSize, thump, payload, channel, context);
    }

    @Override
    public void handleMechanisms(int frameSize, SaslMechanisms saslMechanisms, AMQPTestDriver context) {
        doVerification(frameSize, saslMechanisms, null, 0, context);
    }

    @Override
    public void handleInit(int frameSize, SaslInit saslInit, AMQPTestDriver context) {
        doVerification(frameSize, saslInit, null, 0, context);
    }

    @Override
    public void handleChallenge(int frameSize, SaslChallenge saslChallenge, AMQPTestDriver context) {
        doVerification(frameSize, saslChallenge, null, 0, context);
    }

    @Override
    public void handleResponse(int frameSize, SaslResponse saslResponse, AMQPTestDriver context) {
        doVerification(frameSize, saslResponse, null, 0, context);
    }

    @Override
    public void handleOutcome(int frameSize, SaslOutcome saslOutcome, AMQPTestDriver context) {
        doVerification(frameSize, saslOutcome, null, 0, context);
    }

    @Override
    public void handleAMQPHeader(AMQPHeader header, AMQPTestDriver context) {
        doVerification(header.getBuffer().length, header, null, 0, context);
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, AMQPTestDriver context) {
        doVerification(header.getBuffer().length, header, null, 0, context);
    }

    //----- Internal implementation

    @SuppressWarnings("unchecked")
    private void doVerification(int frameSize, Object performative, ByteBuffer payload, int channel, AMQPTestDriver driver) {
        if (getExpectedTypeClass().equals(performative.getClass())) {
            verifyFrameSize(frameSize);
            verifyPayload(payload);
            verifyChannel(channel);
            verifyPerformative(getExpectedTypeClass().cast(performative));

            if (verificationPredicate != null) {
                try {
                    if (!verificationPredicate.test((T) performative)) {
                        throw new AssertionError("Failed to pass user supplied performative verification predicate");
                    }
                } catch (AssertionError e) {
                    throw e;
                } catch (Throwable t) {
                    throw new AssertionError("Performative should not have been sent with a payload: ");
                }
            }

            if (performativeCapture != null) {
                try {
                    performativeCapture.accept((T) performative);
                } catch (Exception e) {
                    // Ignored
                }
            }

        } else {
            reportTypeExpectationError(performative, getExpectedTypeClass());
        }
    }

    private void reportTypeExpectationError(Object received, Class<T> expected) {
        throw new UnexpectedPerformativeError("Expected type: " + expected + " but received value: " + received);
    }
}
