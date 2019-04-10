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

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Begin performative
 */
public class BeginExpectation extends AbstractExpectation<Begin> {

    /**
     * Enumeration which maps to fields in the Begin Performative
     */
    public enum Field {
        REMOTE_CHANNEL,
        NEXT_OUTGOING_ID,
        INCOMING_WINDOW,
        OUTGOING_WINDOW,
        HANDLE_MAX,
        OFFERED_CAPABILITIES,
        DESIRED_CAPABILITIES,
        PROPERTIES,
    }

    private BeginInjectAction response;

    public BeginExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public BeginExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public BeginInjectAction respond() {
        response = new BeginInjectAction();
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleBegin(Begin begin, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        super.handleBegin(begin, payload, channel, context);

        if (response == null) {
            return;
        }

        // Input was validated now populate response with auto values where not configured
        // to say otherwise by the test.
        if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
            // TODO - We could track session in the driver and therefore allocate
            //        free channels based on activity during the test.  For now we
            //        are simply mirroring the channels back.
            response.onChannel(channel);
        }

        // Populate the remote channel with that of the incoming begin unless scripted to send
        // otherwise which can indicate error handling test.
        if (!response.getPerformative().hasRemoteChannel()) {
            response.getPerformative().setRemoteChannel(channel);
        }

        if (!response.getPerformative().hasNextOutgoingId()) {
            response.getPerformative().setNextOutgoingId(begin.getNextOutgoingId());
        }
        if (!response.getPerformative().hasIncomingWindow()) {
            response.getPerformative().setIncomingWindow(begin.getIncomingWindow());
        }
        if (!response.getPerformative().hasOutgoingWindow()) {
            response.getPerformative().setOutgoingWindow(begin.getOutgoingWindow());
        }
        if (!response.getPerformative().hasHandleMax()) {
            response.getPerformative().setHandleMax(begin.getHandleMax());
        }

        // The remainder of the values are left not set unless set in the test script
    }

    //----- Type specific with methods that perform simple equals checks

    public BeginExpectation withRemoteChannel(int remoteChannel) {
        return withRemoteChannel(equalTo(remoteChannel));
    }

    public BeginExpectation withNextOutgoingId(long nextOutgoingId) {
        return withNextOutgoingId(equalTo(nextOutgoingId));
    }

    public BeginExpectation withIncomingWindow(long incomingWindow) {
        return withIncomingWindow(equalTo(incomingWindow));
    }

    public BeginExpectation withOutgoingWindow(long outgoingWindow) {
        return withOutgoingWindow(equalTo(outgoingWindow));
    }

    public BeginExpectation withHandleMax(long handleMax) {
        return withHandleMax(equalTo(handleMax));
    }

    public BeginExpectation withOfferedCapabilities(Symbol... offeredCapabilities) {
        return withOfferedCapabilities(equalTo(offeredCapabilities));
    }

    public BeginExpectation withDesiredCapabilities(Symbol... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public BeginExpectation withProperties(Map<Symbol, Object> properties) {
        return withProperties(equalTo(properties));
    }

    //----- Matcher based with methods for more complex validation

    public BeginExpectation withRemoteChannel(Matcher<?> m) {
        getMatchers().put(Field.REMOTE_CHANNEL, m);
        return this;
    }

    public BeginExpectation withNextOutgoingId(Matcher<?> m) {
        getMatchers().put(Field.NEXT_OUTGOING_ID, m);
        return this;
    }

    public BeginExpectation withIncomingWindow(Matcher<?> m) {
        getMatchers().put(Field.INCOMING_WINDOW, m);
        return this;
    }

    public BeginExpectation withOutgoingWindow(Matcher<?> m) {
        getMatchers().put(Field.OUTGOING_WINDOW, m);
        return this;
    }

    public BeginExpectation withHandleMax(Matcher<?> m) {
        getMatchers().put(Field.HANDLE_MAX, m);
        return this;
    }

    public BeginExpectation withOfferedCapabilities(Matcher<?> m) {
        getMatchers().put(Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public BeginExpectation withDesiredCapabilities(Matcher<?> m) {
        getMatchers().put(Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public BeginExpectation withProperties(Matcher<?> m) {
        getMatchers().put(Field.PROPERTIES, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Begin begin, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.REMOTE_CHANNEL) {
            result = begin.hasRemoteChannel() ? begin.getRemoteChannel() : null;
        } else if (performativeField == Field.NEXT_OUTGOING_ID) {
            result = begin.hasNextOutgoingId() ? begin.getNextOutgoingId() : null;
        } else if (performativeField == Field.INCOMING_WINDOW) {
            result = begin.hasIncomingWindow() ? begin.getIncomingWindow() : null;
        } else if (performativeField == Field.OUTGOING_WINDOW) {
            result = begin.hasOutgoingWindow() ? begin.getOutgoingWindow() : null;
        } else if (performativeField == Field.HANDLE_MAX) {
            result = begin.hasHandleMax() ? begin.getHandleMax() : null;
        } else if (performativeField == Field.OFFERED_CAPABILITIES) {
            result = begin.hasOfferedCapabilites() ? begin.getOfferedCapabilities() : null;
        } else if (performativeField == Field.DESIRED_CAPABILITIES) {
            result = begin.hasDesiredCapabilites() ? begin.getDesiredCapabilities() : null;
        } else if (performativeField == Field.PROPERTIES) {
            result = begin.hasProperties() ? begin.getProperties() : null;
        } else {
            throw new AssertionError("Request for unknown field in type Begin");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<Begin> getExpectedTypeClass() {
        return Begin.class;
    }
}
