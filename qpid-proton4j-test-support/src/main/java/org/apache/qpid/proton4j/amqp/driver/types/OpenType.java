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
package org.apache.qpid.proton4j.amqp.driver.types;

import static org.hamcrest.CoreMatchers.equalTo;

import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.ScriptedExpectation.ExpectationBuilder;
import org.apache.qpid.proton4j.amqp.driver.ScriptedExpectation.ResponseBuilder;
import org.apache.qpid.proton4j.amqp.driver.actions.OpenInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.OpenExpectation;
import org.apache.qpid.proton4j.amqp.transport.Open;

/**
 * AMQP Header Type
 */
public class OpenType {

    private OpenType() {}

    //----- Static methods for easier scripting

    public static OpenExpectationBuilder open() {
        return new OpenExpectationBuilder(new OpenExpectation());
    }

    public static void injectLater(AMQPTestDriver driver, Open open) {
        driver.addScriptedElement(new OpenInjectAction(open, 0));
    }

    public static void injectLater(AMQPTestDriver driver, Open open, int channel) {
        driver.addScriptedElement(new OpenInjectAction(open, channel));
    }

    public static void injectNow(AMQPTestDriver driver, Open open) {
        driver.sendAMQPFrame(0, open, null);
    }

    public static void injectNow(AMQPTestDriver driver, Open open, int channel) {
        driver.sendAMQPFrame(channel, open, null);
    }

    //----- Builder for this expectation

    public static class OpenExpectationBuilder implements ExpectationBuilder {

        private final OpenExpectation expected;

        public OpenExpectationBuilder(OpenExpectation expected) {
            this.expected = expected;
        }

        public OpenExpectationBuilder withContainerId(String containerId) {
            expected.withContainerId(equalTo(containerId));
            return this;
        }

        public OpenExpectationBuilder withHostname(String hostname) {
            expected.withHostname(equalTo(hostname));
            return this;
        }

        public OpenExpectationBuilder withMaxFrameSize(UnsignedInteger maxFrameSize) {
            expected.withHostname(equalTo(maxFrameSize));
            return this;
        }

        public OpenExpectationBuilder withChannelMax(UnsignedShort channelMax) {
            expected.withHostname(equalTo(channelMax));
            return this;
        }

        public OpenExpectationBuilder withIdleTimeOut(UnsignedInteger idleTimeout) {
            expected.withHostname(equalTo(idleTimeout));
            return this;
        }

        public OpenExpectationBuilder withOutgoingLocales(Symbol... outgoingLocales) {
            expected.withHostname(equalTo(outgoingLocales));
            return this;
        }

        public OpenExpectationBuilder withIncomingLocales(Symbol... incomingLocales) {
            expected.withHostname(equalTo(incomingLocales));
            return this;
        }

        public OpenExpectationBuilder withOfferedCapabilities(Symbol... offeredCapabilities) {
            expected.withHostname(equalTo(offeredCapabilities));
            return this;
        }

        public OpenExpectationBuilder withDesiredCapabilities(Symbol... desiredCapabilities) {
            expected.withHostname(equalTo(desiredCapabilities));
            return this;
        }

        public OpenExpectationBuilder withProperties(Map<Symbol, Object> properties) {
            expected.withHostname(equalTo(properties));
            return this;
        }

        @Override
        public OpenResponseBuilder expect(AMQPTestDriver driver) {
            driver.addScriptedElement(expected);

            return new OpenResponseBuilder();
        }
    }

    public static class OpenResponseBuilder implements ResponseBuilder {

        private final Open open = new Open();
        private int channel = 0;

        public OpenResponseBuilder onChannel(int channel) {
            this.channel = channel;
            return this;
        }

        public OpenResponseBuilder withContainerId(String containerId) {
            open.setContainerId(containerId);
            return this;
        }

        public OpenResponseBuilder withHostname(String hostname) {
            open.setHostname(hostname);
            return this;
        }

        public OpenResponseBuilder withMaxFrameSize(UnsignedInteger maxFrameSize) {
            open.setMaxFrameSize(maxFrameSize);
            return this;
        }

        public OpenResponseBuilder withChannelMax(UnsignedShort channelMax) {
            open.setChannelMax(channelMax);
            return this;
        }

        public OpenResponseBuilder withIdleTimeOut(UnsignedInteger idleTimeout) {
            open.setIdleTimeOut(idleTimeout);
            return this;
        }

        public OpenResponseBuilder withOutgoingLocales(Symbol... outgoingLocales) {
            open.setOutgoingLocales(outgoingLocales);
            return this;
        }

        public OpenResponseBuilder withIncomingLocales(Symbol... incomingLocales) {
            open.setIncomingLocales(incomingLocales);
            return this;
        }

        public OpenResponseBuilder withOfferedCapabilities(Symbol... offeredCapabilities) {
            open.setOfferedCapabilities(offeredCapabilities);
            return this;
        }

        public OpenResponseBuilder withDesiredCapabilities(Symbol... desiredCapabilities) {
            open.setDesiredCapabilities(desiredCapabilities);
            return this;
        }

        public OpenResponseBuilder withProperties(Map<Symbol, Object> properties) {
            open.setProperties(properties);
            return this;
        }

        @Override
        public void respond(AMQPTestDriver driver) {
            driver.addScriptedElement(new OpenInjectAction(open, channel));
        }
    }
}
