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

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.DetachInjectAction;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Detach performative
 */
public class DetachExpectation extends AbstractExpectation<Detach> {

    /**
     * Enumeration which maps to fields in the Detach Performative
     */
    public enum Field {
        HANDLE,
        CLOSED,
        ERROR
    }

    DetachInjectAction response;

    public DetachExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public DetachExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public DetachInjectAction respond() {
        response = new DetachInjectAction(new Detach());
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleDetach(Detach detach, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        super.handleDetach(detach, payload, channel, context);

        if (response == null) {
            return;
        }

        // Input was validated now populate response with auto values where not configured
        // to say otherwise by the test.
        if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
            response.onChannel(channel);
        }

        if (!response.getPerformative().hasHandle()) {
            response.getPerformative().setHandle(detach.getHandle());
        }

        if (!response.getPerformative().hasClosed()) {
            response.getPerformative().setClosed(detach.getClosed());
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public DetachExpectation withHandle(long handle) {
        return withHandle(equalTo(handle));
    }

    public DetachExpectation withClosed(boolean closed) {
        return withClosed(equalTo(closed));
    }

    public DetachExpectation withError(ErrorCondition error) {
        return withError(equalTo(error));
    }

    //----- Matcher based with methods for more complex validation

    public DetachExpectation withHandle(Matcher<?> m) {
        getMatchers().put(Field.HANDLE, m);
        return this;
    }

    public DetachExpectation withClosed(Matcher<?> m) {
        getMatchers().put(Field.CLOSED, m);
        return this;
    }

    public DetachExpectation withError(Matcher<?> m) {
        getMatchers().put(Field.ERROR, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Detach end, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.HANDLE) {
            result = end.hasHandle() ? end.getHandle() : null;
        } else if (performativeField == Field.CLOSED) {
            result = end.hasClosed() ? end.getClosed() : null;
        } else if (performativeField == Field.ERROR) {
            result = end.hasError() ? end.getError() : null;
        } else {
            throw new AssertionError("Request for unknown field in type Detach");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<Detach> getExpectedTypeClass() {
        return Detach.class;
    }
}
