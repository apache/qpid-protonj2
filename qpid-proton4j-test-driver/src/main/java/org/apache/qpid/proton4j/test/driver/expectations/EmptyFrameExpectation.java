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

import org.apache.qpid.proton4j.test.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.test.driver.actions.EmptyFrameInjectAction;
import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.test.driver.codec.transport.HeartBeat;
import org.apache.qpid.proton4j.test.driver.matchers.transport.HeartBeatMatcher;
import org.hamcrest.Matcher;

/**
 * Expectation that asserts an Empty AMQP frame arrived (heart beat).
 */
public class EmptyFrameExpectation extends AbstractExpectation<HeartBeat> {

    private final HeartBeatMatcher matcher = new HeartBeatMatcher();

    public EmptyFrameExpectation(AMQPTestDriver driver) {
        super(driver);
        onChannel(0);
    }

    @Override
    public EmptyFrameExpectation onChannel(int channel) {
        if (channel != 0) throw new IllegalArgumentException("Empty Frames must arrive on channel zero");
        super.onChannel(0);
        return this;
    }

    public EmptyFrameInjectAction respond() {
        EmptyFrameInjectAction response = new EmptyFrameInjectAction(driver);
        driver.addScriptedElement(response);
        return response;
    }

    @Override
    protected Matcher<ListDescribedType> getExpectationMatcher() {
        return matcher;
    }

    @Override
    protected Object getFieldValue(HeartBeat received, Enum<?> performativeField) {
        throw new AssertionError("Should not be matching on any fields, heart beat frame is empty");
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        throw new AssertionError("Should not be matching on any fields, heart beat frame is empty");
    }

    @Override
    protected Class<HeartBeat> getExpectedTypeClass() {
        return HeartBeat.class;
    }
}
