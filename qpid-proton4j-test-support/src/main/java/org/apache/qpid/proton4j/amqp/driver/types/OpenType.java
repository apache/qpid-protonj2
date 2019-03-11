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
import org.apache.qpid.proton4j.amqp.driver.actions.OpenInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.OpenExpectation;
import org.apache.qpid.proton4j.amqp.transport.Open;

/**
 * AMQP Header Type
 */
public class OpenType {

    private OpenType() {}

    //----- Static methods for easier scripting

    public static OpenExpectation expectOpen(AMQPTestDriver driver) {
        OpenExpectation expecting = new OpenExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static OpenInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new Open(), 0);
    }

    public static OpenInjectAction injectLater(AMQPTestDriver driver, Open open) {
        return injectLater(driver, open, 0);
    }

    public static OpenInjectAction injectLater(AMQPTestDriver driver, Open open, int channel) {
        OpenInjectAction inject = new OpenInjectAction(open, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, Open open) {
        driver.sendAMQPFrame(0, open, null);
    }

    public static void injectNow(AMQPTestDriver driver, Open open, int channel) {
        driver.sendAMQPFrame(channel, open, null);
    }
}
