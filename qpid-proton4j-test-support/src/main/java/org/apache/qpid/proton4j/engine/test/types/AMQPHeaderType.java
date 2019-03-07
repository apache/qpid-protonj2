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
package org.apache.qpid.proton4j.engine.test.types;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.engine.test.EngineTestDriver;
import org.apache.qpid.proton4j.engine.test.ScriptedExpectation.ExpectationBuilder;
import org.apache.qpid.proton4j.engine.test.ScriptedExpectation.ResponseBuilder;
import org.apache.qpid.proton4j.engine.test.actions.AMQPHeaderInjectAction;
import org.apache.qpid.proton4j.engine.test.expectations.AMQPHeaderExpectation;

/**
 * AMQP Header Type
 */
public class AMQPHeaderType {

    private AMQPHeaderType() {}

    //----- Static methods for easier scripting

    public static AMQPHeaderExpectationBuilder raw() {
        return new AMQPHeaderExpectationBuilder(new AMQPHeaderExpectation(AMQPHeader.getAMQPHeader()));
    }

    public static AMQPHeaderExpectationBuilder sasl() {
        return new AMQPHeaderExpectationBuilder(new AMQPHeaderExpectation(AMQPHeader.getSASLHeader()));
    }

    public static void scriptInject(EngineTestDriver driver, AMQPHeader header) {
        driver.addScriptedElement(new AMQPHeaderInjectAction(header));
    }

    public static void injectNow(EngineTestDriver driver, AMQPHeader header) {
        driver.sendHeader(header);
    }

    //----- Builder for this expectation

    public static class AMQPHeaderResponseBuilder implements ResponseBuilder {

        private AMQPHeader response;

        public AMQPHeaderResponseBuilder raw() {
            response = AMQPHeader.getAMQPHeader();
            return this;
        }

        public AMQPHeaderResponseBuilder sasl() {
            response = AMQPHeader.getSASLHeader();
            return this;
        }

        @Override
        public void respond(EngineTestDriver driver) {
            driver.addScriptedElement(new AMQPHeaderInjectAction(response));
        }
    }

    public static class AMQPHeaderExpectationBuilder implements ExpectationBuilder {

        private final AMQPHeaderExpectation expected;

        public AMQPHeaderExpectationBuilder(AMQPHeaderExpectation expected) {
            this.expected = expected;
        }

        @Override
        public AMQPHeaderResponseBuilder expect(EngineTestDriver driver) {
            driver.addScriptedElement(expected);

            return new AMQPHeaderResponseBuilder();
        }
    }
}
