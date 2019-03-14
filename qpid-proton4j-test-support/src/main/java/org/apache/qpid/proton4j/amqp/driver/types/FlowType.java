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
import org.apache.qpid.proton4j.amqp.driver.actions.FlowInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.FlowExpectation;
import org.apache.qpid.proton4j.amqp.transport.Flow;

/**
 * AMQP Flow type
 */
public class FlowType {

    private FlowType() {}

    //----- Static methods for easier scripting

    public static FlowExpectation expect(AMQPTestDriver driver) {
        FlowExpectation expecting = new FlowExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static FlowInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new Flow(), 0);
    }

    public static FlowInjectAction injectLater(AMQPTestDriver driver, Flow flow) {
        return injectLater(driver, flow, 0);
    }

    public static FlowInjectAction injectLater(AMQPTestDriver driver, Flow flow, int channel) {
        FlowInjectAction inject = new FlowInjectAction(flow, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, Flow flow) {
        driver.sendAMQPFrame(0, flow, null);
    }

    public static void injectNow(AMQPTestDriver driver, Flow flow, int channel) {
        driver.sendAMQPFrame(channel, flow, null);
    }
}
