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
import org.apache.qpid.proton4j.test.driver.LinkTracker;
import org.apache.qpid.proton4j.test.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.test.driver.actions.DetachInjectAction;
import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.transport.Detach;
import org.apache.qpid.proton4j.test.driver.codec.transport.ErrorCondition;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.test.driver.matchers.transport.DetachMatcher;
import org.hamcrest.Matcher;

import io.netty.buffer.ByteBuf;

/**
 * Scripted expectation for the AMQP Detach performative
 */
public class DetachExpectation extends AbstractExpectation<Detach> {

    private final DetachMatcher matcher = new DetachMatcher();

    private DetachInjectAction response;

    public DetachExpectation(AMQPTestDriver driver) {
        super(driver);
    }

    @Override
    public DetachExpectation onChannel(int channel) {
        super.onChannel(channel);
        return this;
    }

    public DetachInjectAction respond() {
        response = new DetachInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    //----- Handle the performative and configure response is told to respond

    @Override
    public void handleDetach(Detach detach, ByteBuf payload, int channel, AMQPTestDriver context) {
        super.handleDetach(detach, payload, channel, context);

        LinkTracker link = driver.getSessions().handleDetach(detach, channel);

        if (response == null) {
            return;
        }

        // Input was validated now populate response with auto values where not configured
        // to say otherwise by the test.
        if (response.onChannel() == BeginInjectAction.CHANNEL_UNSET) {
            response.onChannel(link.getSession().getLocalChannel());
        }

        if (response.getPerformative().getHandle() == null) {
            response.withHandle(detach.getHandle());
        }

        if (response.getPerformative().getClosed() == null) {
            response.withClosed(detach.getClosed());
        }
    }

    //----- Type specific with methods that perform simple equals checks

    public DetachExpectation withHandle(int handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public DetachExpectation withHandle(long handle) {
        return withHandle(equalTo(UnsignedInteger.valueOf(handle)));
    }

    public DetachExpectation withHandle(UnsignedInteger handle) {
        return withHandle(equalTo(handle));
    }

    public DetachExpectation withClosed(boolean closed) {
        return withClosed(equalTo(closed));
    }

    public DetachExpectation withError(ErrorCondition error) {
        return withError(equalTo(error));
    }

    public DetachExpectation withError(String condition, String description) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition), description)));
    }

    public DetachExpectation withError(String condition, String description, Map<String, Object> info) {
        return withError(equalTo(new ErrorCondition(Symbol.valueOf(condition), description, TypeMapper.toSymbolKeyedMap(info))));
    }

    public DetachExpectation withError(Symbol condition, String description) {
        return withError(equalTo(new ErrorCondition(condition, description)));
    }

    public DetachExpectation withError(Symbol condition, String description, Map<Symbol, Object> info) {
        return withError(equalTo(new ErrorCondition(condition, description, info)));
    }

    //----- Matcher based with methods for more complex validation

    public DetachExpectation withHandle(Matcher<?> m) {
        matcher.addFieldMatcher(Detach.Field.HANDLE, m);
        return this;
    }

    public DetachExpectation withClosed(Matcher<?> m) {
        matcher.addFieldMatcher(Detach.Field.CLOSED, m);
        return this;
    }

    public DetachExpectation withError(Matcher<?> m) {
        matcher.addFieldMatcher(Detach.Field.ERROR, m);
        return this;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Class<Detach> getExpectedTypeClass() {
        return Detach.class;
    }
}
