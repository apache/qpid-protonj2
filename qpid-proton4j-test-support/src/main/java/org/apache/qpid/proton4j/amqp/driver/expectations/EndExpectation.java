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
import org.apache.qpid.proton4j.amqp.driver.actions.EndInjectAction;
import org.apache.qpid.proton4j.amqp.driver.codec.transport.End;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.driver.matchers.transport.EndMatcher;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP End performative
 */
public class EndExpectation extends AbstractExpectation<End> {

    private final EndMatcher matcher = new EndMatcher();

    private EndInjectAction response;

    public EndExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public EndExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public EndInjectAction respond() {
        response = new EndInjectAction();
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleEnd(End end, ProtonBuffer payload, int channel, AMQPTestDriver context) {
        super.handleEnd(end, payload, channel, context);

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

    public EndExpectation withError(ErrorCondition error) {
        return withError(equalTo(TypeMapper.mapFromProtonType(error)));
    }

    //----- Matcher based with methods for more complex validation

    public EndExpectation withError(Matcher<?> m) {
        matcher.addFieldMatcher(End.Field.ERROR, m);
        return this;
    }

    @Override
    protected Object getFieldValue(End end, Enum<?> performativeField) {
        return end.getFieldValue(performativeField.ordinal());
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return End.Field.values()[fieldIndex];
    }

    @Override
    protected Class<End> getExpectedTypeClass() {
        return End.class;
    }
}
