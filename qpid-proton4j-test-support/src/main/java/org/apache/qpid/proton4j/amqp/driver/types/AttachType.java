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
import org.apache.qpid.proton4j.amqp.driver.actions.AttachInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.AttachExpectation;
import org.apache.qpid.proton4j.amqp.transport.Attach;

/**
 * AMQP Attach Type
 */
public class AttachType {

    private AttachType() {}

    //----- Static methods for easier scripting

    public static AttachExpectation expect(AMQPTestDriver driver) {
        AttachExpectation expecting = new AttachExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static AttachInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new Attach(), 0);
    }

    public static AttachInjectAction injectLater(AMQPTestDriver driver, Attach attach) {
        return injectLater(driver, attach, 0);
    }

    public static AttachInjectAction injectLater(AMQPTestDriver driver, Attach attach, int channel) {
        AttachInjectAction inject = new AttachInjectAction(attach, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, Attach attach) {
        driver.sendAMQPFrame(0, attach, null);
    }

    public static void injectNow(AMQPTestDriver driver, Attach attach, int channel) {
        driver.sendAMQPFrame(channel, attach, null);
    }
}
