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
package org.apache.qpid.protonj2.test.driver.expectations;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;

import org.apache.qpid.protonj2.test.driver.AMQPTestDriver;
import org.apache.qpid.protonj2.test.driver.SessionTracker;
import org.apache.qpid.protonj2.test.driver.actions.BeginInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.EndInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedShort;
import org.apache.qpid.protonj2.test.driver.codec.transport.End;
import org.apache.qpid.protonj2.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.protonj2.test.driver.codec.util.TypeMapper;
import org.apache.qpid.protonj2.test.driver.matchers.transport.EndMatcher;
import org.hamcrest.Matcher;

import io.netty5.buffer.Buffer;

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
    public void handleEnd(int frameSize, End end, Buffer payload, int channel, AMQPTestDriver context) {
        super.handleEnd(frameSize, end, payload, channel, context);

        // Ensure that local session tracking knows that remote ended a Session.
        final SessionTracker session = context.sessions().handleEnd(end, UnsignedShort.valueOf(channel));

        if (response != null) {
            if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
                response.onChannel(session.getLocalChannel());
            }
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
