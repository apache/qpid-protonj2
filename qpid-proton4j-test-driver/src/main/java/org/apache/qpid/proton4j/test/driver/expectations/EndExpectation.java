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
package org.apache.qpid.proton4j.test.driver.expectations;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;

import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.SessionTracker;
import org.apache.qpid.proton4j.test.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.test.driver.actions.EndInjectAction;
import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.transport.End;
import org.apache.qpid.proton4j.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.test.driver.matchers.transport.EndMatcher;
import org.hamcrest.Matcher;

import io.netty.buffer.ByteBuf;

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
        response = new EndInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleEnd(End end, ByteBuf payload, int channel, AMQPTestDriver context) {
        super.handleEnd(end, payload, channel, context);

        SessionTracker session = context.getSessions().handleEnd(end, channel);

        if (response == null) {
            return;
        }

        // Input was validated now populate response with auto values where not configured
        // to say otherwise by the test.
        if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
            response.onChannel(session.getLocalChannel());
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public EndExpectation withError(ErrorCondition error) {
        return withError(equalTo(error));
    }

    public EndExpectation withError(String condition, String description) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition), description)));
    }

    public EndExpectation withError(String condition, String description, Map<String, Object> info) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info))));
    }

    public EndExpectation withError(Symbol condition, String description) {
        return withError(equalTo(new ErrorCondition(condition, description)));
    }

    public EndExpectation withError(Symbol condition, String description, Map<Symbol, Object> info) {
        return withError(equalTo(new ErrorCondition(condition, description, info)));
    }

    //----- Matcher based with methods for more complex validation

    public EndExpectation withError(Matcher<?> m) {
        matcher.addFieldMatcher(End.Field.ERROR, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Class<End> getExpectedTypeClass() {
        return End.class;
    }
}
