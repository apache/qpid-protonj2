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

import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Flow performative
 */
public class FlowExpectation extends AbstractExceptation<Flow> {

    /**
     * Enumeration which maps to fields in the Flow Performative
     */
    public enum Field {
        NEXT_INCOMING_ID,
        INCOMING_WINDOW,
        NEXT_OUTGOING_ID,
        OUTGOING_WINDOW,
        HANDLE,
        DELIVERY_COUNT,
        LINK_CREDIT,
        AVAILABLE,
        DRAIN,
        ECHO,
        PROPERTIES,
    }

    public FlowExpectation withNextIncomingId(Matcher<?> m) {
        getMatchers().put(Field.NEXT_INCOMING_ID, m);
        return this;
    }

    public FlowExpectation withIncomingWindow(Matcher<?> m) {
        getMatchers().put(Field.INCOMING_WINDOW, m);
        return this;
    }

    public FlowExpectation withNextOutgoingId(Matcher<?> m) {
        getMatchers().put(Field.NEXT_OUTGOING_ID, m);
        return this;
    }

    public FlowExpectation withOutgoingWindow(Matcher<?> m) {
        getMatchers().put(Field.OUTGOING_WINDOW, m);
        return this;
    }

    public FlowExpectation withHandle(Matcher<?> m) {
        getMatchers().put(Field.HANDLE, m);
        return this;
    }

    public FlowExpectation withDeliveryCount(Matcher<?> m) {
        getMatchers().put(Field.DELIVERY_COUNT, m);
        return this;
    }

    public FlowExpectation withLinkCredit(Matcher<?> m) {
        getMatchers().put(Field.LINK_CREDIT, m);
        return this;
    }

    public FlowExpectation withAvailable(Matcher<?> m) {
        getMatchers().put(Field.AVAILABLE, m);
        return this;
    }

    public FlowExpectation withDrain(Matcher<?> m) {
        getMatchers().put(Field.DRAIN, m);
        return this;
    }

    public FlowExpectation withEcho(Matcher<?> m) {
        getMatchers().put(Field.ECHO, m);
        return this;
    }

    public FlowExpectation withProperties(Matcher<?> m) {
        getMatchers().put(Field.PROPERTIES, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Flow flow, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.NEXT_INCOMING_ID) {
            result = flow.hasNextIncomingId() ? flow.getNextIncomingId() : null;
        } else if (performativeField == Field.INCOMING_WINDOW) {
            result = flow.hasIncomingWindow() ? flow.getIncomingWindow() : null;
        } else if (performativeField == Field.NEXT_OUTGOING_ID) {
            result = flow.hasNextOutgoingId() ? flow.getNextOutgoingId() : null;
        } else if (performativeField == Field.OUTGOING_WINDOW) {
            result = flow.hasOutgoingWindow() ? flow.getOutgoingWindow() : null;
        } else if (performativeField == Field.HANDLE) {
            result = flow.hasHandle() ? flow.getHandle() : null;
        } else if (performativeField == Field.DELIVERY_COUNT) {
            result = flow.hasDeliveryCount() ? flow.getDeliveryCount() : null;
        } else if (performativeField == Field.LINK_CREDIT) {
            result = flow.hasLinkCredit() ? flow.getLinkCredit() : null;
        } else if (performativeField == Field.AVAILABLE) {
            result = flow.hasAvailable() ? flow.getAvailable() : null;
        } else if (performativeField == Field.DRAIN) {
            result = flow.hasDrain() ? flow.getDrain() : null;
        } else if (performativeField == Field.ECHO) {
            result = flow.hasEcho() ? flow.getEcho() : null;
        } else if (performativeField == Field.PROPERTIES) {
            result = flow.hasProperties() ? flow.getProperties() : null;
        } else {
            throw new AssertionError("Request for unknown field in type Flow");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<Flow> getExpectedTypeClass() {
        return Flow.class;
    }
}
