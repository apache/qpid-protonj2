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
package org.apache.qpid.protonj2.test.driver;

import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import org.apache.qpid.protonj2.test.driver.actions.AMQPHeaderInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.AttachInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.BeginInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.CloseInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.DeclareInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.DetachInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.DetachLastCoordinatorInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.DischargeInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.DispositionInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.EmptyFrameInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.EndInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.ExecuteUserCodeAction;
import org.apache.qpid.protonj2.test.driver.actions.FlowInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.OpenInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.RawBytesInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.SaslChallengeInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.SaslInitInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.SaslMechanismsInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.SaslOutcomeInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.SaslResponseInjectAction;
import org.apache.qpid.protonj2.test.driver.actions.TransferInjectAction;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Source;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Target;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslCode;
import org.apache.qpid.protonj2.test.driver.codec.transactions.Coordinator;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.codec.transport.Role;
import org.apache.qpid.protonj2.test.driver.expectations.AMQPHeaderExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.AttachExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.BeginExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.CloseExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.DeclareExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.DetachExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.DischargeExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.DispositionExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.EmptyFrameExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.EndExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.FlowExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.OpenExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.SaslChallengeExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.SaslInitExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.SaslMechanismsExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.SaslOutcomeExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.SaslResponseExpectation;
import org.apache.qpid.protonj2.test.driver.expectations.TransferExpectation;
import org.hamcrest.Matchers;

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
    public abstract AMQPTestDriver getDriver();

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

    //----- Transaction expectations

    public AttachExpectation expectCoordinatorAttach() {
        AttachExpectation expecting = new AttachExpectation(getDriver());

        expecting.withRole(Role.SENDER);
        expecting.withCoordinator(isA(Coordinator.class));
        expecting.withSource(notNullValue());

        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public DeclareExpectation expectDeclare() {
        DeclareExpectation expecting = new DeclareExpectation(getDriver());

        expecting.withHandle(notNullValue());
        expecting.withDeliveryId(notNullValue());
        expecting.withDeliveryTag(notNullValue());
        expecting.withMessageFormat(Matchers.oneOf(null, 0, UnsignedInteger.ZERO));

        getDriver().addScriptedElement(expecting);
        return expecting;
    }

    public DischargeExpectation expectDischarge() {
        DischargeExpectation expecting = new DischargeExpectation(getDriver());

        expecting.withHandle(notNullValue());
        expecting.withDeliveryId(notNullValue());
        expecting.withDeliveryTag(notNullValue());
        expecting.withMessageFormat(Matchers.oneOf(null, 0, UnsignedInteger.ZERO));

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

    public AMQPHeaderInjectAction remoteHeader(byte[] header) {
        return new AMQPHeaderInjectAction(getDriver(), new AMQPHeader(header));
    }

    public AMQPHeaderInjectAction remoteHeader(AMQPHeader header) {
        return new AMQPHeaderInjectAction(getDriver(), header);
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

    public DetachInjectAction remoteDetachLastCoordinatorLink() {
        return new DetachLastCoordinatorInjectAction(getDriver());
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

    public DeclareInjectAction remoteDeclare() {
        return new DeclareInjectAction(getDriver());
    }

    public DischargeInjectAction remoteDischarge() {
        return new DischargeInjectAction(getDriver());
    }

    public EmptyFrameInjectAction remoteEmptyFrame() {
        return new EmptyFrameInjectAction(getDriver());
    }

    public RawBytesInjectAction remoteBytes() {
        return new RawBytesInjectAction(getDriver());
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
        expectSaslInit().withMechanism("PLAIN").withInitialResponse(saslPlainInitialResponse(username, password));
        remoteSaslOutcome().withCode(SaslCode.OK).queue();
        expectAMQPHeader().respondWithAMQPHeader();
    }

    /**
     * Creates all the scripted elements needed for a successful SASL XOAUTH2
     * connection.
     * <p>
     * For this exchange the SASL header is expected which is responded to with the
     * corresponding SASL header and an immediate SASL mechanisms frame that only
     * advertises XOAUTH2 as the mechanism.  It is expected that the remote will
     * send a SASL init with the XOAUTH2 mechanism selected and the outcome is
     * predefined as success.  Once done the expectation is added for the AMQP
     * header to arrive and a header response will be sent.
     *
     * @param username
     *      The user name that is expected in the SASL initial response.
     * @param password
     *      The password that is expected in the SASL initial response.
     */
    public void expectSaslXOauth2Connect(String username, String password) {
        expectSASLHeader().respondWithSASLPHeader();
        remoteSaslMechanisms().withMechanisms("XOAUTH2").queue();
        expectSaslInit().withMechanism("XOAUTH2").withInitialResponse(saslXOauth2InitialResponse(username, password));
        remoteSaslOutcome().withCode(SaslCode.OK).queue();
        expectAMQPHeader().respondWithAMQPHeader();
    }

    /**
     * Creates all the scripted elements needed for a failed SASL Plain
     * connection.
     * <p>
     * For this exchange the SASL header is expected which is responded to with the
     * corresponding SASL header and an immediate SASL mechanisms frame that only
     * advertises plain as the mechanism.  It is expected that the remote will
     * send a SASL init with the plain mechanism selected and the outcome is
     * predefined failing the exchange.
     *
     * @param saslCode
     *      The SASL code that indicates which failure the remote will be sent.
     */
    public void expectFailingSASLPlainConnect(byte saslCode) {
        expectFailingSASLPlainConnect(saslCode, "PLAIN");
    }

    /**
     * Creates all the scripted elements needed for a failed SASL Plain
     * connection.
     * <p>
     * For this exchange the SASL header is expected which is responded to with the
     * corresponding SASL header and an immediate SASL mechanisms frame that only
     * advertises plain as the mechanism.  It is expected that the remote will
     * send a SASL init with the plain mechanism selected and the outcome is
     * predefined failing the exchange.
     *
     * @param saslCode
     *      The SASL code that indicates which failure the remote will be sent.
     * @param offeredMechanisms
     *      The set of mechanisms that the server should offer in the SASL Mechanisms frame
     */
    public void expectFailingSASLPlainConnect(byte saslCode, String... offeredMechanisms) {
        assertTrue(Arrays.asList(offeredMechanisms).contains("PLAIN"));

        expectSASLHeader().respondWithSASLPHeader();
        remoteSaslMechanisms().withMechanisms(offeredMechanisms).queue();
        expectSaslInit().withMechanism("PLAIN");

        if (saslCode <= 0 || saslCode > SaslCode.SYS_TEMP.ordinal()) {
            throw new IllegalArgumentException("SASL Code should indicate a failure");
        }

        remoteSaslOutcome().withCode(SaslCode.valueOf(saslCode)).queue();
    }

    /**
     * Creates all the scripted elements needed for a successful SASL EXTERNAL
     * connection.
     * <p>
     * For this exchange the SASL header is expected which is responded to with the
     * corresponding SASL header and an immediate SASL mechanisms frame that only
     * advertises EXTERNAL as the mechanism.  It is expected that the remote will
     * send a SASL init with the EXTERNAL mechanism selected and the outcome is
     * predefined as success.  Once done the expectation is added for the AMQP
     * header to arrive and a header response will be sent.
     */
    public void expectSaslExternalConnect() {
        expectSASLHeader().respondWithSASLPHeader();
        remoteSaslMechanisms().withMechanisms("EXTERNAL").queue();
        expectSaslInit().withMechanism("EXTERNAL").withInitialResponse(new byte[0]);
        remoteSaslOutcome().withCode(SaslCode.OK).queue();
        expectAMQPHeader().respondWithAMQPHeader();
    }

    /**
     * Creates all the scripted elements needed for a SASL exchange with the offered
     * mechanisms but the client should fail if configured such that it cannot match
     * any of those to its own available mechanisms.
     *
     * @param offeredMechanisms
     *      The set of SASL Mechanisms to advertise as available on the peer.
     */
    public void expectSaslMechanismNegotiationFailure(String... offeredMechanisms) {
        expectSASLHeader().respondWithSASLPHeader();
        remoteSaslMechanisms().withMechanisms(offeredMechanisms).queue();
    }

    /**
     * Creates all the scripted elements needed for a SASL exchange with the offered
     * mechanisms with the expectation that the client will respond with the provided
     * mechanism and then the server will fail the exchange with the auth failed code.
     *
     * @param offeredMechanisms
     *      The set of SASL Mechanisms to advertise as available on the peer.
     * @param chosenMechanism
     *      The SASL Mechanism that the client should select and respond with.
     */
    public void expectSaslConnectThatAlwaysFailsAuthentication(String[] offeredMechanisms, String chosenMechanism) {
        expectSASLHeader().respondWithSASLPHeader();
        remoteSaslMechanisms().withMechanisms(offeredMechanisms).queue();
        expectSaslInit().withMechanism(chosenMechanism);
        remoteSaslOutcome().withCode(SaslCode.AUTH).queue();
    }

    //----- Utility methods for tests writing raw scripted SASL tests

    public byte[] saslPlainInitialResponse(String username, String password) {
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] initialResponse = new byte[usernameBytes.length+passwordBytes.length+2];
        System.arraycopy(usernameBytes, 0, initialResponse, 1, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, initialResponse, 2 + usernameBytes.length, passwordBytes.length);

        return initialResponse;
    }

    public byte[] saslXOauth2InitialResponse(String username, String password) {
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] initialResponse = new byte[usernameBytes.length+passwordBytes.length+20];

        System.arraycopy("user=".getBytes(StandardCharsets.US_ASCII), 0, initialResponse, 0, 5);
        System.arraycopy(usernameBytes, 0, initialResponse, 5, usernameBytes.length);
        initialResponse[5 + usernameBytes.length] = 1;
        System.arraycopy("auth=Bearer ".getBytes(StandardCharsets.US_ASCII), 0, initialResponse, 6+usernameBytes.length, 12);
        System.arraycopy(passwordBytes, 0, initialResponse, 18 + usernameBytes.length, passwordBytes.length);
        initialResponse[initialResponse.length - 2] = 1;
        initialResponse[initialResponse.length - 1] = 1;

        return initialResponse;
    }

    //----- Smart Scripted Response Actions

    /**
     * Creates a Begin response for the last session Begin that was received and fills in the Begin
     * fields based on values from the remote.  The caller can further customize the Begin that is
     * emitted by using the various with methods to assign values to the fields in the Begin.
     *
     * @return a new {@link BeginInjectAction} that can be queued or sent immediately.
     *
     * @throws IllegalStateException if no Begin has yet been received from the remote.
     */
    public BeginInjectAction respondToLastBegin() {
        BeginInjectAction response = new BeginInjectAction(getDriver());

        SessionTracker session = getDriver().sessions().getLastRemotelyOpenedSession();
        if (session == null) {
            throw new IllegalStateException("Cannot create response to Begin before one has been received.");
        }

        // Populate the response using data in the locally opened session, script can override this after return.
        response.withRemoteChannel(session.getRemoteChannel());

        return response;
    }

    /**
     * Creates a Attach response for the last link Attach that was received and fills in the Attach
     * fields based on values from the remote.  The caller can further customize the Attach that is
     * emitted by using the various with methods to assign values to the fields in the Attach.
     *
     * @return a new {@link AttachInjectAction} that can be queued or sent immediately.
     *
     * @throws IllegalStateException if no Attach has yet been received from the remote.
     */
    public AttachInjectAction respondToLastAttach() {
        AttachInjectAction response = new AttachInjectAction(getDriver());

        final SessionTracker session = getDriver().sessions().getLastRemotelyOpenedSession();
        final LinkTracker link = session.getLastRemotelyOpenedLink();

        if (link == null) {
            throw new IllegalStateException("Cannot create response to Attach before one has been received.");
        }

        if (link.isLocallyAttached()) {
            throw new IllegalStateException("Cannot create response to Attach since a local Attach was already sent.");
        }

        // Populate the response using data in the locally opened link, script can override this after return.
        response.onChannel(link.getSession().getLocalChannel());
        response.withName(link.getName());
        response.withRole(link.getRole());
        response.withSndSettleMode(link.getRemoteSenderSettleMode());
        response.withRcvSettleMode(link.getRemoteReceiverSettleMode());

        if (link.getRemoteSource() != null) {
            response.withSource(new Source(link.getRemoteSource()));
            if (Boolean.TRUE.equals(link.getRemoteSource().getDynamic())) {
                response.withSource().withAddress(UUID.randomUUID().toString());
            }
        }
        if (link.getRemoteTarget() != null) {
            response.withTarget(new Target(link.getRemoteTarget()));
            if (Boolean.TRUE.equals(link.getRemoteTarget().getDynamic())) {
                response.withTarget().withAddress(UUID.randomUUID().toString());
            }
        }
        if (link.getRemoteCoordinator() != null) {
            response.withTarget(new Coordinator(link.getRemoteCoordinator()));
        }

        if (response.getPerformative().getInitialDeliveryCount() == null) {
            if (link.isSender()) {
                response.withInitialDeliveryCount(0);
            }
        }

        return response;
    }

    //----- Out of band script actions for user code

    public ExecuteUserCodeAction execute(Runnable action) {
        return new ExecuteUserCodeAction(getDriver(), action);
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
