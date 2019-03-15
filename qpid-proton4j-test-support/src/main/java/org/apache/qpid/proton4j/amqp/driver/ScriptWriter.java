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
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Attach;
import org.apache.qpid.proton4j.amqp.transport.Begin;
import org.apache.qpid.proton4j.amqp.transport.Close;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.Disposition;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.Flow;
import org.apache.qpid.proton4j.amqp.transport.Open;
import org.apache.qpid.proton4j.amqp.transport.Transfer;

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

    //------ AMQP Header scripting methods

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

    public void injectLater(AMQPHeader header) {
        driver.addScriptedElement(new AMQPHeaderInjectAction(header));
    }

    public void injectNow(AMQPHeader header) {
        driver.sendHeader(header);
    }

    //------ AMQP Open scripting methods

    public OpenExpectation expectOpen() {
        OpenExpectation expecting = new OpenExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public OpenInjectAction injectLater(Open open) {
        return injectLater(open, 0);
    }

    public OpenInjectAction injectLater(Open open, int channel) {
        OpenInjectAction inject = new OpenInjectAction(open, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(Open open) {
        driver.sendAMQPFrame(0, open, null);
    }

    public void injectNow(Open open, int channel) {
        driver.sendAMQPFrame(channel, open, null);
    }

    //------ AMQP Close scripting methods

    public CloseExpectation expectClose() {
        CloseExpectation expecting = new CloseExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public CloseInjectAction injectLater(Close close) {
        return injectLater(close, 0);
    }

    public CloseInjectAction injectLater(Close close, int channel) {
        CloseInjectAction inject = new CloseInjectAction(close, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(Close close) {
        driver.sendAMQPFrame(0, close, null);
    }

    public void injectNow(Close close, int channel) {
        driver.sendAMQPFrame(channel, close, null);
    }

    //------ AMQP Begin scripting methods

    public BeginExpectation expectBegin() {
        BeginExpectation expecting = new BeginExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public BeginInjectAction injectLater(Begin begin) {
        return injectLater(begin, 0);
    }

    public BeginInjectAction injectLater(Begin begin, int channel) {
        BeginInjectAction inject = new BeginInjectAction(begin, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(Begin begin) {
        driver.sendAMQPFrame(0, begin, null);
    }

    public void injectNow(Begin begin, int channel) {
        driver.sendAMQPFrame(channel, begin, null);
    }

    //------ AMQP End scripting methods

    public EndExpectation expectEnd() {
        EndExpectation expecting = new EndExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public EndInjectAction injectLater(End end) {
        return injectLater(end, 0);
    }

    public EndInjectAction injectLater(End end, int channel) {
        EndInjectAction inject = new EndInjectAction(end, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(End end) {
        driver.sendAMQPFrame(0, end, null);
    }

    public void injectNow(End end, int channel) {
        driver.sendAMQPFrame(channel, end, null);
    }

    //------ AMQP End scripting methods

    public AttachExpectation expectAttach() {
        AttachExpectation expecting = new AttachExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public AttachInjectAction injectLater( Attach attach) {
        return injectLater(attach, 0);
    }

    public AttachInjectAction injectLater(Attach attach, int channel) {
        AttachInjectAction inject = new AttachInjectAction(attach, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(Attach attach) {
        driver.sendAMQPFrame(0, attach, null);
    }

    public void injectNow(Attach attach, int channel) {
        driver.sendAMQPFrame(channel, attach, null);
    }

    //------ AMQP End scripting methods

    public DetachExpectation expectDetach() {
        DetachExpectation expecting = new DetachExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public DetachInjectAction injectLater(Detach detach) {
        return injectLater(detach, 0);
    }

    public DetachInjectAction injectLater(Detach detach, int channel) {
        DetachInjectAction inject = new DetachInjectAction(detach, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(Detach detach) {
        driver.sendAMQPFrame(0, detach, null);
    }

    public void injectNow(Detach detach, int channel) {
        driver.sendAMQPFrame(channel, detach, null);
    }

    //------ AMQP Flow scripting methods

    public FlowExpectation expectFlow() {
        FlowExpectation expecting = new FlowExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public FlowInjectAction injectLater(Flow flow) {
        return injectLater(flow, 0);
    }

    public FlowInjectAction injectLater(Flow flow, int channel) {
        FlowInjectAction inject = new FlowInjectAction(flow, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(Flow flow) {
        driver.sendAMQPFrame(0, flow, null);
    }

    public void injectNow(Flow flow, int channel) {
        driver.sendAMQPFrame(channel, flow, null);
    }

    //------ AMQP Transfer scripting methods

    public TransferExpectation expectTransfer() {
        TransferExpectation expecting = new TransferExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public TransferInjectAction injectLater(Transfer transfer) {
        return injectLater(transfer, 0);
    }

    public TransferInjectAction injectLater(Transfer transfer, int channel) {
        TransferInjectAction inject = new TransferInjectAction(transfer, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(Transfer transfer) {
        driver.sendAMQPFrame(0, transfer, null);
    }

    public void injectNow( Transfer transfer, int channel) {
        driver.sendAMQPFrame(channel, transfer, null);
    }

    //------ AMQP Disposition scripting methods

    public DispositionExpectation expectDisposition() {
        DispositionExpectation expecting = new DispositionExpectation(driver);
        driver.addScriptedElement(expecting);
        return expecting;
    }

    public DispositionInjectAction injectLater(Disposition disposition) {
        return injectLater(disposition, 0);
    }

    public DispositionInjectAction injectLater(Disposition disposition, int channel) {
        DispositionInjectAction inject = new DispositionInjectAction(disposition, channel);
        driver.addScriptedElement(inject);
        return inject;
    }

    public void injectNow(Disposition disposition) {
        driver.sendAMQPFrame(0, disposition, null);
    }

    public void injectNow(Disposition disposition, int channel) {
        driver.sendAMQPFrame(channel, disposition, null);
    }
}
