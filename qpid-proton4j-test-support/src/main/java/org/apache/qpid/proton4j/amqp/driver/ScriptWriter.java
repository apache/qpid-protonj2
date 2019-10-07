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

import java.nio.charset.StandardCharsets;

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
import org.apache.qpid.proton4j.amqp.driver.actions.SaslChallengeInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.SaslInitInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.SaslMechanismsInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.SaslOutcomeInjectAction;
import org.apache.qpid.proton4j.amqp.driver.actions.SaslResponseInjectAction;
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
import org.apache.qpid.proton4j.amqp.driver.expectations.SaslChallengeExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.SaslInitExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.SaslMechanismsExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.SaslOutcomeExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.SaslResponseExpectation;
import org.apache.qpid.proton4j.amqp.driver.expectations.TransferExpectation;
import org.apache.qpid.proton4j.amqp.security.SaslCode;
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

    //----- AMQP Performative expectations

    public AMQPHeaderExpectation expectAMQPHeader() {
        AMQPHeaderExpectation expecting = new AMQPHeaderExpectation(AMQPHeader.getAMQPHeader(), getDriver());
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

    //----- SASL performative expectations

    public AMQPHeaderExpectation expectSASLHeader() {
        AMQPHeaderExpectation expecting = new AMQPHeaderExpectation(AMQPHeader.getSASLHeader(), getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public SaslMechanismsExpectation expectSaslMechanisms() {
        SaslMechanismsExpectation expecting = new SaslMechanismsExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public SaslInitExpectation expectSaslInit() {
        SaslInitExpectation expecting = new SaslInitExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public SaslChallengeExpectation expectSaslChallenge() {
        SaslChallengeExpectation expecting = new SaslChallengeExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public SaslResponseExpectation expectSaslResponse() {
        SaslResponseExpectation expecting = new SaslResponseExpectation(getDriver());
        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public SaslOutcomeExpectation expectSaslOutcome() {
        SaslOutcomeExpectation expecting = new SaslOutcomeExpectation(getDriver());
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

    //----- Remote SASL operations that can be scripted during tests

    public SaslInitInjectAction remoteSaslInit() {
        return new SaslInitInjectAction(getDriver());
    }

    public SaslMechanismsInjectAction remoteSaslMechanisms() {
        return new SaslMechanismsInjectAction(getDriver());
    }

    public SaslChallengeInjectAction remoteSaslChallenge() {
        return new SaslChallengeInjectAction(getDriver());
    }

    public SaslResponseInjectAction remoteSaslResponse() {
        return new SaslResponseInjectAction(getDriver());
    }

    public SaslOutcomeInjectAction remoteSaslOutcome() {
        return new SaslOutcomeInjectAction(getDriver());
    }

    //----- SASL related test expectations

    /**
     * Creates all the scripted elements needed for a successful SASL Anonymous
     * connection.
     * <p>
     * For this exchange the SASL header is expected which is responded to with the
     * corresponding SASL header and an immediate SASL mechanisms frame that only
     * advertises anonymous as the mechanism.  It is expected that the remote will
     * send a SASL init with the anonymous mechanism selected and the outcome is
     * predefined as success.  Once done the expectation is added for the AMQP
     * header to arrive and a header response will be sent.
     */
    public void expectSASLAnonymousConnect() {
        expectSASLHeader().respondWithSASLPHeader();
        remoteSaslMechanisms().withMechanisms("ANONYMOUS").queue();
        expectSaslInit().withMechanism("ANONYMOUS");
        remoteSaslOutcome().withCode(SaslCode.OK).queue();
        expectAMQPHeader().respondWithAMQPHeader();
    }

    /**
     * Creates all the scripted elements needed for a successful SASL Plain
     * connection.
     * <p>
     * For this exchange the SASL header is expected which is responded to with the
     * corresponding SASL header and an immediate SASL mechanisms frame that only
     * advertises plain as the mechanism.  It is expected that the remote will
     * send a SASL init with the plain mechanism selected and the outcome is
     * predefined as success.  Once done the expectation is added for the AMQP
     * header to arrive and a header response will be sent.
     *
     * @param username
     *      The user name that is expected in the SASL Plain initial response.
     * @param password
     *      The password that is expected in the SASL Plain initial response.
     */
    public void expectSASLPlainConnect(String username, String password) {
        expectSASLHeader().respondWithSASLPHeader();
        remoteSaslMechanisms().withMechanisms("PLAIN").queue();

        // SASL PLAIN initial response encoding
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] initialResponse = new byte[usernameBytes.length+passwordBytes.length+2];
        System.arraycopy(usernameBytes, 0, initialResponse, 1, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, initialResponse, 2 + usernameBytes.length, passwordBytes.length);

        expectSaslInit().withMechanism("PLAIN").withInitialResponse(initialResponse);
        remoteSaslOutcome().withCode(SaslCode.OK).queue();
        expectAMQPHeader().respondWithAMQPHeader();
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
