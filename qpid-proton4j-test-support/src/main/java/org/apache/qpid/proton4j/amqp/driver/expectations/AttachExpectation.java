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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.actions.AttachInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.Target;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.apache.qpid.proton4j.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Attach performative
 */
public class AttachExpectation extends AbstractExpectation<Attach> {

    /**
     * Enumeration which maps to fields in the Attach Performative
     */
    public enum Field {
        NAME,
        HANDLE,
        ROLE,
        SND_SETTLE_MODE,
        RCV_SETTLE_MODE,
        SOURCE,
        TARGET,
        UNSETTLED,
        INCOMPLETE_UNSETTLED,
        INITIAL_DELIVERY_COUNT,
        MAX_MESSAGE_SIZE,
        OFFERED_CAPABILITIES,
        DESIRED_CAPABILITIES,
        PROPERTIES
    }

    private AttachInjectAction response;

    public AttachExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public AttachExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public AttachInjectAction respond() {
        response = new AttachInjectAction();
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleAttach(Attach attach, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        super.handleAttach(attach, payload, channel, context);

        if (response == null) {
            return;
        }

        // Input was validated now populate response with auto values where not configured
        // to say otherwise by the test.
        if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
            // TODO - We could track attach in the driver and therefore allocate
            //        free channels based on activity during the test.  For now we
            //        are simply mirroring the channels back.
            response.onChannel(channel);
        }

        // Populate the fields of the response with defaults if non set by the test script
        if (response.getPerformative().getHandle() == null) {
            response.withHandle(attach.getHandle());
        }
        if (response.getPerformative().getName() == null) {
            response.withName(attach.getName());
        }
        if (response.getPerformative().getRole() == null) {
            response.withRole(attach.getRole() == Role.SENDER ? Role.RECEIVER : Role.SENDER);
        }
        if (response.getPerformative().getSndSettleMode() == null) {
            response.withSndSettleMode(attach.getSndSettleMode());
        }
        if (response.getPerformative().getRcvSettleMode() == null) {
            response.withRcvSettleMode(attach.getRcvSettleMode());
        }
        if (response.getPerformative().getSource() == null) {
            response.withSource(attach.getSource());
        }
        if (response.getPerformative().getTarget() == null) {
            response.withTarget(attach.getTarget());
        }

        // Other fields are left not set for now unless test script configured
    }

    //----- Type specific with methods that perform simple equals checks

    public AttachExpectation withName(String name) {
        return withName(equalTo(name));
    }

    public AttachExpectation withHandle(long handle) {
        return withHandle(equalTo(handle));
    }

    public AttachExpectation withRole(Role role) {
        return withRole(equalTo(role));
    }

    public AttachExpectation withSndSettleMode(SenderSettleMode sndSettleMode) {
        return withSndSettleMode(equalTo(sndSettleMode));
    }

    public AttachExpectation withRcvSettleMode(ReceiverSettleMode rcvSettleMode) {
        return withRcvSettleMode(equalTo(rcvSettleMode));
    }

    public AttachExpectation withSource(Source source) {
        return withSource(equalTo(source));
    }

    public AttachExpectation withTarget(Target target) {
        return withTarget(equalTo(target));
    }

    public AttachExpectation withUnsettled(Map<Binary, DeliveryState> unsettled) {
        return withUnsettled(equalTo(unsettled));
    }

    public AttachExpectation withIncompleteUnsettled(boolean incomplete) {
        return withIncompleteUnsettled(equalTo(incomplete));
    }

    public AttachExpectation withInitialDeliveryCount(long initialDeliveryCount) {
        return withInitialDeliveryCount(equalTo(initialDeliveryCount));
    }

    public AttachExpectation withMaxMessageSize(UnsignedLong maxMessageSize) {
        return withMaxMessageSize(equalTo(maxMessageSize));
    }

    public AttachExpectation withOfferedCapabilities(Symbol... offeredCapabilities) {
        return withOfferedCapabilities(equalTo(offeredCapabilities));
    }

    public AttachExpectation withDesiredCapabilities(Symbol... desiredCapabilities) {
        return withDesiredCapabilities(equalTo(desiredCapabilities));
    }

    public AttachExpectation withProperties(Map<Symbol, Object> properties) {
        return withProperties(equalTo(properties));
    }

    //----- Matcher based with methods for more complex validation

    public AttachExpectation withName(Matcher<?> m) {
        getMatchers().put(Field.NAME, m);
        return this;
    }

    public AttachExpectation withHandle(Matcher<?> m) {
        getMatchers().put(Field.HANDLE, m);
        return this;
    }

    public AttachExpectation withRole(Matcher<?> m) {
        getMatchers().put(Field.ROLE, m);
        return this;
    }

    public AttachExpectation withSndSettleMode(Matcher<?> m) {
        getMatchers().put(Field.SND_SETTLE_MODE, m);
        return this;
    }

    public AttachExpectation withRcvSettleMode(Matcher<?> m) {
        getMatchers().put(Field.RCV_SETTLE_MODE, m);
        return this;
    }

    public AttachExpectation withSource(Matcher<?> m) {
        getMatchers().put(Field.SOURCE, m);
        return this;
    }

    public AttachExpectation withTarget(Matcher<?> m) {
        getMatchers().put(Field.TARGET, m);
        return this;
    }

    public AttachExpectation withUnsettled(Matcher<?> m) {
        getMatchers().put(Field.UNSETTLED, m);
        return this;
    }

    public AttachExpectation withIncompleteUnsettled(Matcher<?> m) {
        getMatchers().put(Field.INCOMPLETE_UNSETTLED, m);
        return this;
    }

    public AttachExpectation withInitialDeliveryCount(Matcher<?> m) {
        getMatchers().put(Field.INITIAL_DELIVERY_COUNT, m);
        return this;
    }

    public AttachExpectation withMaxMessageSize(Matcher<?> m) {
        getMatchers().put(Field.MAX_MESSAGE_SIZE, m);
        return this;
    }

    public AttachExpectation withOfferedCapabilities(Matcher<?> m) {
        getMatchers().put(Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public AttachExpectation withDesiredCapabilities(Matcher<?> m) {
        getMatchers().put(Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public AttachExpectation withProperties(Matcher<?> m) {
        getMatchers().put(Field.PROPERTIES, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Attach attach, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.NAME) {
            result = attach.hasName() ? attach.getName() : null;
        } else if (performativeField == Field.HANDLE) {
            result = attach.hasHandle() ? attach.getHandle() : null;
        } else if (performativeField == Field.ROLE) {
            result = attach.hasRole() ? attach.getRole() : null;
        } else if (performativeField == Field.SND_SETTLE_MODE) {
            result = attach.hasSenderSettleMode() ? attach.getSndSettleMode(): null;
        } else if (performativeField == Field.RCV_SETTLE_MODE) {
            result = attach.hasReceiverSettleMode() ? attach.getRcvSettleMode() : null;
        } else if (performativeField == Field.SOURCE) {
            result = attach.hasSource() ? attach.getSource() : null;
        } else if (performativeField == Field.TARGET) {
            result = attach.hasTarget() ? attach.getTarget() : null;
        } else if (performativeField == Field.UNSETTLED) {
            result = attach.hasUnsettled() ? attach.getUnsettled() : null;
        } else if (performativeField == Field.INCOMPLETE_UNSETTLED) {
            result = attach.hasIncompleteUnsettled() ? attach.getIncompleteUnsettled() : null;
        } else if (performativeField == Field.INITIAL_DELIVERY_COUNT) {
            result = attach.hasInitialDeliveryCount() ? attach.getInitialDeliveryCount() : null;
        } else if (performativeField == Field.MAX_MESSAGE_SIZE) {
            result = attach.hasMaxMessageSize() ? attach.getMaxMessageSize() : null;
        } else if (performativeField == Field.OFFERED_CAPABILITIES) {
            result = attach.hasOfferedCapabilites() ? attach.getOfferedCapabilities() : null;
        } else if (performativeField == Field.DESIRED_CAPABILITIES) {
            result = attach.hasDesiredCapabilites() ? attach.getDesiredCapabilities() : null;
        } else if (performativeField == Field.PROPERTIES) {
            result = attach.hasProperties() ? attach.getProperties() : null;
        } else {
            throw new AssertionError("Request for unknown field in type Attach");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<Attach> getExpectedTypeClass() {
        return Attach.class;
    }
}
