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
import org.apache.qpid.proton4j.amqp.driver.actions.CloseInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.CloseExpectation;
import org.apache.qpid.proton4j.amqp.transport.Close;

/**
 * AMQP Close type
 */
public class CloseType {

    private CloseType() {}

    //----- Static methods for easier scripting

    public static CloseExpectation expect(AMQPTestDriver driver) {
        CloseExpectation expecting = new CloseExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static CloseInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new Close(), 0);
    }

    public static CloseInjectAction injectLater(AMQPTestDriver driver, Close close) {
        return injectLater(driver, close, 0);
    }

    public static CloseInjectAction injectLater(AMQPTestDriver driver, Close close, int channel) {
        CloseInjectAction inject = new CloseInjectAction(close, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, Close close) {
        driver.sendAMQPFrame(0, close, null);
    }

    public static void injectNow(AMQPTestDriver driver, Close close, int channel) {
        driver.sendAMQPFrame(channel, close, null);
    }
}
