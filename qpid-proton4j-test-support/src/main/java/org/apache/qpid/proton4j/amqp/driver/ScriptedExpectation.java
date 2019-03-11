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
package org.apache.qpid.proton4j.amqp.driver;

/**
 * Entry in the test script that defines an expected output from the AMQP source being
 * tested.
 */
public interface ScriptedExpectation extends ScriptedElement {

    @Override
    default ScriptEntryType getType() {
        return ScriptEntryType.EXPECTATION;
    }

    /**
     * Builder that is used to build up an expectation(s) to be added to a driver instance.
     */
    interface ExpectationBuilder {

        /**
         * Instructs the given driver instance that it should add this expectation to the script
         * of items expected during the test.
         *
         * @param driver
         *      The driver that the expectation should be added to.
         *
         * @return a response builder that can be used to create a scripted response for this expectation.
         */
        ResponseBuilder expect(AMQPTestDriver driver);

    }

    /**
     * Builder that is used to build up an expectation(s) to be added to a driver instance.
     */
    interface ResponseBuilder {

        /**
         * Instructs the given driver instance that it should add this expectation to the script
         * of items expected during the test.
         *
         * @param driver
         *      The driver that the expectation should be added to.
         */
        void respond(AMQPTestDriver driver);

    }
}
