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

import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.driver.actions.AMQPHeaderInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.AttachInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.BeginInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.CloseInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.DetachInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.DispositionInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.EmptyFrameInjectAction;
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
import org.apache.qpid.proton4j.amqp.driver.expectations.EmptyFrameExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.EndExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.FlowExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.OpenExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.TransferExpectation;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;

/**
 * Class used to create test scripts using the {@link AMQPTestDriver}
 */
public abstract class ScriptWriter {

    /**
     * Implemented in the subclass this method returns the active AMQP test driver
     * instance being used for the tests.
     *
     * @return the {@link AMQPTestDriver} to use for building a test script.
     */
    protected abstract AMQPTestDriver getDriver();

    public AMQPHeaderExpectation expectAMQPHeader() {
        AMQPHeaderExpectation expecting = new AMQPHeaderExpectation(AMQPHeader.getAMQPHeader(), getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public AMQPHeaderExpectation expectSASLHeader() {
        AMQPHeaderExpectation expecting = new AMQPHeaderExpectation(AMQPHeader.getSASLHeader(), getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public OpenExpectation expectOpen() {
        OpenExpectation expecting = new OpenExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public CloseExpectation expectClose() {
        CloseExpectation expecting = new CloseExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public BeginExpectation expectBegin() {
        BeginExpectation expecting = new BeginExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public EndExpectation expectEnd() {
        EndExpectation expecting = new EndExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public AttachExpectation expectAttach() {
        AttachExpectation expecting = new AttachExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public DetachExpectation expectDetach() {
        DetachExpectation expecting = new DetachExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public FlowExpectation expectFlow() {
        FlowExpectation expecting = new FlowExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public TransferExpectation expectTransfer() {
        TransferExpectation expecting = new TransferExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public DispositionExpectation expectDisposition() {
        DispositionExpectation expecting = new DispositionExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public EmptyFrameExpectation expectEmptyFrame() {
        EmptyFrameExpectation expecting = new EmptyFrameExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    //----- Remote operations that happen while running the test script

    public void remoteHeader(AMQPHeader header) {
        getDriver().addScriptedElement(new AMQPHeaderInjectAction(getDriver(), header));
    }

    public OpenInjectAction remoteOpen() {
        return new OpenInjectAction(getDriver());
    }

    public CloseInjectAction remoteClose() {
        return new CloseInjectAction(getDriver());
    }

    public BeginInjectAction remoteBegin() {
        return new BeginInjectAction(getDriver());
    }

    public EndInjectAction remoteEnd() {
        return new EndInjectAction(getDriver());
    }

    public AttachInjectAction remoteAttach() {
        return new AttachInjectAction(getDriver());
    }

    public DetachInjectAction remoteDetach() {
        return new DetachInjectAction(getDriver());
    }

    public FlowInjectAction remoteFlow() {
        return new FlowInjectAction(getDriver());
    }

    public TransferInjectAction remoteTransfer() {
        return new TransferInjectAction(getDriver());
    }

    public DispositionInjectAction remoteDisposition() {
        return new DispositionInjectAction(getDriver());
    }

    public EmptyFrameInjectAction remoteEmptyFrame() {
        return new EmptyFrameInjectAction(getDriver());
    }

    //----- Immediate operations performed outside the test script

    public void fire(AMQPHeader header) {
        getDriver().sendHeader(header);
    }

    public void fireAMQP(DescribedType performative) {
        getDriver().sendAMQPFrame(0, performative, null);
    }

    public void fireSASL(DescribedType performative) {
        getDriver().sendSaslFrame(0, performative);
    }
}
