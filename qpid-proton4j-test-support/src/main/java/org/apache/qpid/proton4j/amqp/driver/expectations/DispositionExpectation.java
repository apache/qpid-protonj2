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
import org.apache.qpid.proton4j.amqp.driver.actions.DispositionInjectAction;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.Role;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Disposition performative
 */
public class DispositionExpectation extends AbstractExpectation<Disposition> {

    /**
     * Enumeration which maps to fields in the Disposition Performative
     */
    public enum Field {
        ROLE,
        FIRST,
        LAST,
        SETTLED,
        STATE,
        BATCHABLE
    }

    public DispositionExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    public DispositionInjectAction respond() {
        DispositionInjectAction response = new DispositionInjectAction(new Disposition(), 0);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Type specific with methods that perform simple equals checks

    public DispositionExpectation withRole(Role role) {
        withRole(equalTo(role));
        return this;
    }

    public DispositionExpectation withFirst(long first) {
        withFirst(equalTo(first));
        return this;
    }

    public DispositionExpectation withLast(long last) {
        withLast(equalTo(last));
        return this;
    }

    public DispositionExpectation withSettled(boolean settled) {
        withSettled(equalTo(settled));
        return this;
    }

    public DispositionExpectation withState(DeliveryState state) {
        withState(equalTo(state));
        return this;
    }

    public DispositionExpectation withBatchable(boolean batchable) {
        withBatchable(equalTo(batchable));
        return this;
    }

    //----- Matcher based with methods for more complex validation

    public DispositionExpectation withRole(Matcher<?> m) {
        getMatchers().put(Field.ROLE, m);
        return this;
    }

    public DispositionExpectation withFirst(Matcher<?> m) {
        getMatchers().put(Field.FIRST, m);
        return this;
    }

    public DispositionExpectation withLast(Matcher<?> m) {
        getMatchers().put(Field.LAST, m);
        return this;
    }

    public DispositionExpectation withSettled(Matcher<?> m) {
        getMatchers().put(Field.SETTLED, m);
        return this;
    }

    public DispositionExpectation withState(Matcher<?> m) {
        getMatchers().put(Field.STATE, m);
        return this;
    }

    public DispositionExpectation withBatchable(Matcher<?> m) {
        getMatchers().put(Field.BATCHABLE, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Disposition disposition, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.ROLE) {
            result = disposition.hasRole() ? disposition.getRole() : null;
        } else if (performativeField == Field.FIRST) {
            result = disposition.hasFirst() ? disposition.getFirst() : null;
        } else if (performativeField == Field.LAST) {
            result = disposition.hasLast() ? disposition.getLast() : null;
        } else if (performativeField == Field.SETTLED) {
            result = disposition.hasSettled() ? disposition.getSettled() : null;
        } else if (performativeField == Field.STATE) {
            result = disposition.hasState() ? disposition.getState() : null;
        } else if (performativeField == Field.BATCHABLE) {
            result = disposition.hasBatchable() ? disposition.getBatchable() : null;
        } else {
            throw new AssertionError("Request for unknown field in type Disposition");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<Disposition> getExpectedTypeClass() {
        return Disposition.class;
    }
}
