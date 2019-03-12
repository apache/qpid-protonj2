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
import org.apache.qpid.proton4j.amqp.driver.actions.DispositionInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.DispositionExpectation;
import org.apache.qpid.proton4j.amqp.transport.Disposition;

/**
 * AMQP {@link Disposition} type
 */
public class DispositionType {

    private DispositionType() {}

    //----- Static methods for easier scripting

    public static DispositionExpectation expectEnd(AMQPTestDriver driver) {
        DispositionExpectation expecting = new DispositionExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static DispositionInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new Disposition(), 0);
    }

    public static DispositionInjectAction injectLater(AMQPTestDriver driver, Disposition disposition) {
        return injectLater(driver, disposition, 0);
    }

    public static DispositionInjectAction injectLater(AMQPTestDriver driver, Disposition disposition, int channel) {
        DispositionInjectAction inject = new DispositionInjectAction(disposition, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, Disposition disposition) {
        driver.sendAMQPFrame(0, disposition, null);
    }

    public static void injectNow(AMQPTestDriver driver, Disposition disposition, int channel) {
        driver.sendAMQPFrame(channel, disposition, null);
    }
}
