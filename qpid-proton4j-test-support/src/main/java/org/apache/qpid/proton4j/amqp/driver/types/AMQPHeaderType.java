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

import org.apache.qpid.proton4j.amqp.driver.AMQPTestDriver;
import org.apache.qpid.proton4j.amqp.driver.actions.AMQPHeaderInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.AMQPHeaderExpectation;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;

/**
 * AMQP Header Type
 */
public class AMQPHeaderType {

    private AMQPHeaderType() {}

    //----- Static methods for easier scripting

    public static AMQPHeaderExpectation expectAMQPHeader(AMQPTestDriver driver) {
        AMQPHeaderExpectation expecting = new AMQPHeaderExpectation(AMQPHeader.getAMQPHeader(), driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static AMQPHeaderExpectation expectSASLHeader(AMQPTestDriver driver) {
        AMQPHeaderExpectation expecting = new AMQPHeaderExpectation(AMQPHeader.getSASLHeader(), driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static void injectLater(AMQPTestDriver driver, AMQPHeader header) {
        driver.addScriptedElement(new AMQPHeaderInjectAction(header));
    }

    public static void injectNow(AMQPTestDriver driver, AMQPHeader header) {
        driver.sendHeader(header);
    }
}
