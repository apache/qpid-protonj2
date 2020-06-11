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

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.CloseInjectAction;
import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.Close;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.driver.matchers.transport.CloseMatcher;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Close performative
 */
public class CloseExpectation extends AbstractExpectation<Close> {

    private final CloseMatcher matcher = new CloseMatcher();

    private CloseInjectAction response;

    public CloseExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    public CloseInjectAction respond() {
        response = new CloseInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleClose(Close close, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        super.handleClose(close, payload, channel, context);

        if (response == null) {
            return;
        }

        // Input was validated now populate response with auto values where not configured
        // to say otherwise by the test.
        if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
            response.onChannel(channel);
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public CloseExpectation withError(ErrorCondition error) {
        return withError(equalTo(TypeMapper.mapFromProtonType(error)));
    }

    public CloseExpectation withError(String condition, String description) {
        return withError(equalTo(
            TypeMapper.mapFromProtonType(new ErrorCondition(Symbol.valueOf(condition), description))));
    }

    public CloseExpectation withError(String condition, String description, Map<String, Object> info) {
        return withError(equalTo(
            TypeMapper.mapFromProtonType(
                new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info)))));
    }

    public CloseExpectation withError(Symbol condition, String description) {
        return withError(equalTo(TypeMapper.mapFromProtonType(new ErrorCondition(condition, description))));
    }

    public CloseExpectation withError(Symbol condition, String description, Map<Symbol, Object> info) {
        return withError(equalTo(TypeMapper.mapFromProtonType(new ErrorCondition(condition, description, info))));
    }

    //----- Matcher based with methods for more complex validation

    public CloseExpectation withError(Matcher<?> m) {
        matcher.addFieldMatcher(Close.Field.ERROR, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Object getFieldValue(Close close, Enum<?> performativeField) {
        return close.getFieldValue(performativeField.ordinal());
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Close.Field.values()[fieldIndex];
    }

    @Override
    protected Class<Close> getExpectedTypeClass() {
        return Close.class;
    }
}
