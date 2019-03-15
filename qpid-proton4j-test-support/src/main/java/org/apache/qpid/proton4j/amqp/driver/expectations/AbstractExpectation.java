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
package org.apache.qpid.proton4j.amqp.driver.expectations;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptedExpectation;
import org.apache.qpid.proton4j.amqp.security.SaslChallenge;
import org.apache.qpid.proton4j.amqp.security.SaslInit;
import org.apache.qpid.proton4j.amqp.security.SaslMechanisms;
import org.apache.qpid.proton4j.amqp.security.SaslOutcome;
import org.apache.qpid.proton4j.amqp.security.SaslResponse;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Transfer;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.common.logging.ProtonLogger;
import org.apache.qpid.proton4j.common.logging.ProtonLoggerFactory;
import org.hamcrest.Matcher;

/**
 * Abstract base for expectations that need to handle matchers against fields in the
 * value being expected.
 *
 * @param <T> The type being validated
 */
public abstract class AbstractExpectation<T> implements ScriptedExpectation {

    private static final ProtonLogger LOG = ProtonLoggerFactory.getLogger(AbstractExpectation.class);

    public static int ANY_CHANNEL = -1;

    protected int expectedChannel = ANY_CHANNEL;
    protected final Map<Enum<?>, Matcher<?>> fieldMatchers = new LinkedHashMap<>();
    protected final AMQPTestDriver driver;

    public AbstractExpectation(AMQPTestDriver driver) {
        this.driver = driver;
    }

    //----- Configure base expectations

    public AbstractExpectation<T> onChannel(int channel) {
        this.expectedChannel = channel;
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
                  "\n  Received:" + performative + "\n  Expectations: " + fieldMatchers);

        for (Map.Entry<Enum<?>, Matcher<?>> entry : fieldMatchers.entrySet()) {
            @SuppressWarnings("unchecked")
            Matcher<Object> matcher = (Matcher<Object>) entry.getValue();
            assertThat("Field " + entry.getKey() + " value should match", getFieldValue(performative, entry.getKey()), matcher);
        }
    }

    protected final void verifyPayload(ProtonBuffer payload) {
        if (payload != null) {
            throw new AssertionError("Performative should not have been sent with a paylod: ");
        }
    }

    protected final void verifyChannel(int channel) {
        if (expectedChannel != ANY_CHANNEL && expectedChannel != channel) {
            throw new AssertionError("Expected send on channel + " + expectedChannel + ": but was on channel:" + channel);
        }
    }

    /**
     * @return the Map of matchers used to validate fields of the received performative.
     */
    protected Map<Enum<?>, Matcher<?>> getMatchers() {
        return fieldMatchers;
    }

    protected abstract Object getFieldValue(T received, Enum<?> performativeField);

    protected abstract Enum<?> getFieldEnum(int fieldIndex);

    protected abstract Class<T> getExpectedTypeClass();

    //----- Base implementation of the handle methods to describe when we get wrong type.

    @Override
    public void handleOpen(Open open, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(open, payload, channel, context);
    }

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(begin, payload, channel, context);
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(attach, payload, channel, context);
    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(flow, payload, channel, context);
    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel,AMQPTestDriver context) {
        doVerification(transfer, payload, channel, context);
    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(disposition, payload, channel, context);
    }

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(detach, payload, channel, context);
    }

    @Override
    public void handleEnd(End end, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(end, payload, channel, context);
    }

    @Override
    public void handleClose(Close close, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        doVerification(close, payload, channel, context);
    }

    @Override
    public void handleMechanisms(SaslMechanisms saslMechanisms, AMQPTestDriver context) {
        doVerification(saslMechanisms, null, 0, context);
    }

    @Override
    public void handleInit(SaslInit saslInit, AMQPTestDriver context) {
        doVerification(saslInit, null, 0, context);
    }

    @Override
    public void handleChallenge(SaslChallenge saslChallenge, AMQPTestDriver context) {
        doVerification(saslChallenge, null, 0, context);
    }

    @Override
    public void handleResponse(SaslResponse saslResponse, AMQPTestDriver context) {
        doVerification(saslResponse, null, 0, context);
    }

    @Override
    public void handleOutcome(SaslOutcome saslOutcome, AMQPTestDriver context) {
        doVerification(saslOutcome, null, 0, context);
    }

    @Override
    public void handleAMQPHeader(AMQPHeader header, AMQPTestDriver context) {
        doVerification(header, null, 0, context);
    }

    @Override
    public void handleSASLHeader(AMQPHeader header, AMQPTestDriver context) {
        doVerification(header, null, 0, context);
    }

    //----- Internal implementation

    private void doVerification(Object performative, ProtonBuffer payload, int channel, AMQPTestDriver driver) {
        if (getExpectedTypeClass().equals(performative.getClass())) {
            verifyPayload(payload);
            verifyChannel(channel);
            verifyPerformative(getExpectedTypeClass().cast(performative));
        } else {
            reportTypeExpectationError(performative, getExpectedTypeClass());
        }
    }

    private void reportTypeExpectationError(Object received, Class<T> expected) {
        throw new AssertionError("Expeceted type: " + expected + " but received value: " + received);
    }
}
