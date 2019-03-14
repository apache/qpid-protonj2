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
import org.apache.qpid.proton4j.amqp.driver.actions.EndInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.EndExpectation;
import org.apache.qpid.proton4j.amqp.transport.End;

/**
 * AMQP End type
 */
public class EndType {

    private EndType() {}

    //----- Static methods for easier scripting

    public static EndExpectation expect(AMQPTestDriver driver) {
        EndExpectation expecting = new EndExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static EndInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new End(), 0);
    }

    public static EndInjectAction injectLater(AMQPTestDriver driver, End end) {
        return injectLater(driver, end, 0);
    }

    public static EndInjectAction injectLater(AMQPTestDriver driver, End end, int channel) {
        EndInjectAction inject = new EndInjectAction(end, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, End end) {
        driver.sendAMQPFrame(0, end, null);
    }

    public static void injectNow(AMQPTestDriver driver, End end, int channel) {
        driver.sendAMQPFrame(channel, end, null);
    }
}
