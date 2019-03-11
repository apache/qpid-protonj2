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
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.actions.OpenInjectAction;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Open performative
 */
public class OpenExpectation extends AbstractExceptation<Open> {

    /**
     * Enumeration which maps to fields in the Open Performative
     */
    public enum Field {
        CONTAINER_ID,
        HOSTNAME,
        MAX_FRAME_SIZE,
        CHANNEL_MAX,
        IDLE_TIME_OUT,
        OUTGOING_LOCALES,
        INCOMING_LOCALES,
        OFFERED_CAPABILITIES,
        DESIRED_CAPABILITIES,
        PROPERTIES,
    }

    private final AMQPTestDriver driver;

    public OpenExpectation(AMQPTestDriver driver) {
        this.expectedChannel = 0;  // Open must used channel zero.
        this.driver = driver;
    }

    public OpenInjectAction respond() {
        OpenInjectAction response = new OpenInjectAction(new Open(), 0);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Type specific with methods that perform simple equals checks

    public OpenExpectation withContainerId(String container) {
        return withContainerId(equalTo(container));
    }

    public OpenExpectation withHostname(String hostname) {
        return withHostname(equalTo(hostname));
    }

    public OpenExpectation withMaxFrameSize(UnsignedInteger maxFrameSize) {
        return withHostname(equalTo(maxFrameSize));
    }

    public OpenExpectation withChannelMax(UnsignedShort channelMax) {
        return withHostname(equalTo(channelMax));
    }

    public OpenExpectation withIdleTimeOut(UnsignedInteger idleTimeout) {
        return withHostname(equalTo(idleTimeout));
    }

    public OpenExpectation withOutgoingLocales(Symbol... outgoingLocales) {
        return withHostname(equalTo(outgoingLocales));
    }

    public OpenExpectation withIncomingLocales(Symbol... incomingLocales) {
        return withHostname(equalTo(incomingLocales));
    }

    public OpenExpectation withOfferedCapabilities(Symbol... offeredCapabilities) {
        return withHostname(equalTo(offeredCapabilities));
    }

    public OpenExpectation withDesiredCapabilities(Symbol... desiredCapabilities) {
        return withHostname(equalTo(desiredCapabilities));
    }

    public OpenExpectation withProperties(Map<Symbol, Object> properties) {
        return withHostname(equalTo(properties));
    }

    //----- Matcher based with methods for more complex validation

    public OpenExpectation withContainerId(Matcher<?> m) {
        getMatchers().put(Field.CONTAINER_ID, m);
        return this;
    }

    public OpenExpectation withHostname(Matcher<?> m) {
        getMatchers().put(Field.HOSTNAME, m);
        return this;
    }

    public OpenExpectation withMaxFrameSize(Matcher<?> m) {
        getMatchers().put(Field.MAX_FRAME_SIZE, m);
        return this;
    }

    public OpenExpectation withChannelMax(Matcher<?> m) {
        getMatchers().put(Field.CHANNEL_MAX, m);
        return this;
    }

    public OpenExpectation withIdleTimeOut(Matcher<?> m) {
        getMatchers().put(Field.IDLE_TIME_OUT, m);
        return this;
    }

    public OpenExpectation withOutgoingLocales(Matcher<?> m) {
        getMatchers().put(Field.OUTGOING_LOCALES, m);
        return this;
    }

    public OpenExpectation withIncomingLocales(Matcher<?> m) {
        getMatchers().put(Field.INCOMING_LOCALES, m);
        return this;
    }

    public OpenExpectation withOfferedCapabilities(Matcher<?> m) {
        getMatchers().put(Field.OFFERED_CAPABILITIES, m);
        return this;
    }

    public OpenExpectation withDesiredCapabilities(Matcher<?> m) {
        getMatchers().put(Field.DESIRED_CAPABILITIES, m);
        return this;
    }

    public OpenExpectation withProperties(Matcher<?> m) {
        getMatchers().put(Field.PROPERTIES, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Open open, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.CONTAINER_ID) {
            result = open.getContainerId();
        } else if (performativeField == Field.HOSTNAME) {
            result = open.getHostname();
        } else if (performativeField == Field.MAX_FRAME_SIZE) {
            result = open.getMaxFrameSize();
        } else if (performativeField == Field.CHANNEL_MAX) {
            result = open.getChannelMax();
        } else if (performativeField == Field.IDLE_TIME_OUT) {
            result = open.getIdleTimeOut();
        } else if (performativeField == Field.OUTGOING_LOCALES) {
            result = open.getOutgoingLocales();
        } else if (performativeField == Field.INCOMING_LOCALES) {
            result = open.getIncomingLocales();
        } else if (performativeField == Field.OFFERED_CAPABILITIES) {
            result = open.getOfferedCapabilities();
        } else if (performativeField == Field.DESIRED_CAPABILITIES) {
            result = open.getDesiredCapabilities();
        } else if (performativeField == Field.PROPERTIES) {
            result = open.getProperties();
        } else {
            throw new AssertionError("Request for unknown field in type Open");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<Open> getExpectedTypeClass() {
        return Open.class;
    }
}
