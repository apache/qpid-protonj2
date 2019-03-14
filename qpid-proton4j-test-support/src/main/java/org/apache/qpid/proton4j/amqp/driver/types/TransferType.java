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
import org.apache.qpid.proton4j.amqp.driver.actions.TransferInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.TransferExpectation;
import org.apache.qpid.proton4j.amqp.transport.Transfer;

/**
 * AMQP Transfer type
 */
public class TransferType {

    private TransferType() {}

    //----- Static methods for easier scripting

    public static TransferExpectation expect(AMQPTestDriver driver) {
        TransferExpectation expecting = new TransferExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public static TransferInjectAction injectLater(AMQPTestDriver driver) {
        return injectLater(driver, new Transfer(), 0);
    }

    public static TransferInjectAction injectLater(AMQPTestDriver driver, Transfer transfer) {
        return injectLater(driver, transfer, 0);
    }

    public static TransferInjectAction injectLater(AMQPTestDriver driver, Transfer transfer, int channel) {
        TransferInjectAction inject = new TransferInjectAction(transfer, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public static void injectNow(AMQPTestDriver driver, Transfer transfer) {
        driver.sendAMQPFrame(0, transfer, null);
    }

    public static void injectNow(AMQPTestDriver driver, Transfer transfer, int channel) {
        driver.sendAMQPFrame(channel, transfer, null);
    }
}
