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
package org.apache.qpid.proton4j.amqp.driver;

import org.apache.qpid.proton4j.amqp.driver.actions.AMQPHeaderInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.AttachInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.CloseInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.DetachInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.DispositionInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.EndInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.FlowInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.OpenInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.TransferInjectAction;
import org.apache.qpid.proton4j.amqp.driver.expectations.AMQPHeaderExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.AttachExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.BeginExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.CloseExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.DetachExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.DispositionExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.EndExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.FlowExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.OpenExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.TransferExpectation;
import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;

/**
 * Class used to create test scripts using the {@link AMQPTestDriver}
 */
public class ScriptWriter {

    private final AMQPTestDriver driver;

    /**
     * @param driver
     *      The test driver that will be used when creating the test script
     */
    public ScriptWriter(AMQPTestDriver driver) {
        this.driver = driver;
    }

    public AMQPHeaderExpectation expectAMQPHeader() {
        AMQPHeaderExpectation expecting = new AMQPHeaderExpectation(AMQPHeader.getAMQPHeader(), driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public AMQPHeaderExpectation expectSASLHeader() {
        AMQPHeaderExpectation expecting = new AMQPHeaderExpectation(AMQPHeader.getSASLHeader(), driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public OpenExpectation expectOpen() {
        OpenExpectation expecting = new OpenExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public CloseExpectation expectClose() {
        CloseExpectation expecting = new CloseExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public BeginExpectation expectBegin() {
        BeginExpectation expecting = new BeginExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public EndExpectation expectEnd() {
        EndExpectation expecting = new EndExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public AttachExpectation expectAttach() {
        AttachExpectation expecting = new AttachExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public DetachExpectation expectDetach() {
        DetachExpectation expecting = new DetachExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public FlowExpectation expectFlow() {
        FlowExpectation expecting = new FlowExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public TransferExpectation expectTransfer() {
        TransferExpectation expecting = new TransferExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public DispositionExpectation expectDisposition() {
        DispositionExpectation expecting = new DispositionExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    //----- Remote operations that happen while running the test script

    public void remoteHeader(AMQPHeader header) {
        driver.addScriptedElement(new AMQPHeaderInjectAction(header));
    }

    public OpenInjectAction remoteOpen() {
        OpenInjectAction inject = new OpenInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    public CloseInjectAction remoteClose() {
        CloseInjectAction inject = new CloseInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    public BeginInjectAction remoteBegin() {
        BeginInjectAction inject = new BeginInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    public EndInjectAction remoteEnd() {
        EndInjectAction inject = new EndInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    public AttachInjectAction remoteAttach() {
        AttachInjectAction inject = new AttachInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    public DetachInjectAction remoteDetach() {
        DetachInjectAction inject = new DetachInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    public FlowInjectAction remoteFlow() {
        FlowInjectAction inject = new FlowInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    public TransferInjectAction remoteTransfer() {
        TransferInjectAction inject = new TransferInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    public DispositionInjectAction remoteDisposition() {
        DispositionInjectAction inject = new DispositionInjectAction();
        driver.addScriptedElement(inject);
        return inject;
    }

    //----- Immediate operations performed outside the test script

    public void fire(AMQPHeader header) {
        driver.sendHeader(header);
    }

    public void fire(Performative performative) {
        driver.sendAMQPFrame(0, performative, null);
    }

    public void fire(SaslPerformative performative) {
        driver.sendSaslFrame(0, performative);
    }
}
