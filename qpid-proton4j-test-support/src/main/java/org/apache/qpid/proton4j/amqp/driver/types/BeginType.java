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
import org.apache.qpid.proton4j.amqp.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.BeginExpectation;
import org.apache.qpid.proton4j.amqp.transport.Begin;

/**
 * AMQP Begin Type
 */
public class BeginType {

    private BeginType() {}

    //----- Static methods for easier scripting

    public static BeginExpectation expectBegin(AMQPTestDriver driver) {
        BeginExpectation expecting = new BeginExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static BeginInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new Begin(), 0);
    }

    public static BeginInjectAction injectLater(AMQPTestDriver driver, Begin begin) {
        return injectLater(driver, begin, 0);
    }

    public static BeginInjectAction injectLater(AMQPTestDriver driver, Begin begin, int channel) {
        BeginInjectAction inject = new BeginInjectAction(begin, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, Begin begin) {
        driver.sendAMQPFrame(0, begin, null);
    }

    public static void injectNow(AMQPTestDriver driver, Begin begin, int channel) {
        driver.sendAMQPFrame(channel, begin, null);
    }
}
