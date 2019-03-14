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
import org.apache.qpid.proton4j.amqp.driver.actions.DetachInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.DetachExpectation;
import org.apache.qpid.proton4j.amqp.transport.Detach;

/**
 * AMQP Detach type
 */
public class DetachType {

    private DetachType() {}

    //----- Static methods for easier scripting

    public static DetachExpectation expect(AMQPTestDriver driver) {
        DetachExpectation expecting = new DetachExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static DetachInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new Detach(), 0);
    }

    public static DetachInjectAction injectLater(AMQPTestDriver driver, Detach detach) {
        return injectLater(driver, detach, 0);
    }

    public static DetachInjectAction injectLater(AMQPTestDriver driver, Detach detach, int channel) {
        DetachInjectAction inject = new DetachInjectAction(detach, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, Detach detach) {
        driver.sendAMQPFrame(0, detach, null);
    }

    public static void injectNow(AMQPTestDriver driver, Detach detach, int channel) {
        driver.sendAMQPFrame(channel, detach, null);
    }
}
