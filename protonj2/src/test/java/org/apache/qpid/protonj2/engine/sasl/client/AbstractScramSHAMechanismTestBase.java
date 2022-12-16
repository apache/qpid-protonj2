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
package org.apache.qpid.protonj2.engine.sasl.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.junit.jupiter.api.Test;

/**
 * The quoted text in the test method javadoc is taken from RFC 5802.
 */
public abstract class AbstractScramSHAMechanismTestBase extends MechanismTestBase {

    private final ProtonBuffer expectedClientInitialResponse;
    private final ProtonBuffer serverFirstMessage;
    private final ProtonBuffer expectedClientFinalMessage;
    private final ProtonBuffer serverFinalMessage;

    public AbstractScramSHAMechanismTestBase(ProtonBuffer expectedClientInitialResponse,
                                             ProtonBuffer serverFirstMessage,
                                             ProtonBuffer expectedClientFinalMessage,
                                             ProtonBuffer serverFinalMessage) {

        this.expectedClientInitialResponse = expectedClientInitialResponse;
        this.serverFirstMessage = serverFirstMessage;
        this.expectedClientFinalMessage = expectedClientFinalMessage;
        this.serverFinalMessage = serverFinalMessage;
    }

    protected abstract Mechanism getMechanismForTesting();

    protected abstract SaslCredentialsProvider getTestCredentials();

    @Test
    public void testSuccessfulAuthentication() throws Exception {
        Mechanism mechanism = getMechanismForTesting();

        ProtonBuffer clientInitialResponse = mechanism.getInitialResponse(getTestCredentials());
        assertEquals(expectedClientInitialResponse, clientInitialResponse);

        ProtonBuffer clientFinalMessage = mechanism.getChallengeResponse(getTestCredentials(), serverFirstMessage);
        assertEquals(expectedClientFinalMessage, clientFinalMessage);

        ProtonBuffer expectedFinalChallengeResponse = ProtonBufferAllocator.defaultAllocator().copy("".getBytes());
        assertEquals(expectedFinalChallengeResponse, mechanism.getChallengeResponse(getTestCredentials(), serverFinalMessage));

        mechanism.verifyCompletion();
    }

    @Test
    public void testServerFirstMessageMalformed() throws Exception {
        Mechanism mechanism = getMechanismForTesting();

        mechanism.getInitialResponse(getTestCredentials());

        ProtonBuffer challenge = ProtonBufferAllocator.defaultAllocator().copy("badserverfirst".getBytes());
        challenge.setWriteOffset(challenge.capacity());

        try {
            mechanism.getChallengeResponse(getTestCredentials(), challenge);
            fail("Exception not thrown");
        } catch (SaslException s) {
            // PASS
        }
    }

    /**
     * 5.1.  SCRAM Attributes
     * "m: This attribute is reserved for future extensibility.  In this
     * version of SCRAM, its presence in a client or a server message
     * MUST cause authentication failure when the attribute is parsed by
     * the other end."
     *
     * @throws Exception if an unexpected exception is thrown.
     */
    @Test
    public void testServerFirstMessageMandatoryExtensionRejected() throws Exception {
        Mechanism mechanism = getMechanismForTesting();

        mechanism.getInitialResponse(getTestCredentials());

        ProtonBuffer challenge = ProtonBufferAllocator.defaultAllocator().copy("m=notsupported,s=,i=".getBytes());
        challenge.setWriteOffset(challenge.capacity());

        try {
            mechanism.getChallengeResponse(getTestCredentials(), challenge);
            fail("Exception not thrown");
        } catch (SaslException s) {
            // PASS
        }
    }

    /**
     * 5.  SCRAM Authentication Exchange
     * "In [the server first] response, the server sends a "server-first-message" containing the
     * user's iteration count i and the user's salt, and appends its own
     * nonce to the client-specified one."
     *
     * @throws Exception if an unexpected exception is thrown.
     */
    @Test
    public void testServerFirstMessageInvalidNonceRejected() throws Exception {
        Mechanism mechanism = getMechanismForTesting();

        mechanism.getInitialResponse(getTestCredentials());

        ProtonBuffer challenge = ProtonBufferAllocator.defaultAllocator().copy(
            "r=invalidnonce,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".getBytes());
        challenge.setWriteOffset(challenge.capacity());

        try {
            mechanism.getChallengeResponse(getTestCredentials(), challenge);
            fail("Exception not thrown");
        } catch (SaslException s) {
            // PASS
        }
    }

    /**
     * 5.  SCRAM Authentication Exchange
     * "The client then authenticates the server by computing the
     * ServerSignature and comparing it to the value sent by the server.  If
     * the two are different, the client MUST consider the authentication
     * exchange to be unsuccessful, and it might have to drop the
     * connection."
     *
     * @throws Exception if an unexpected exception is thrown.
     */
    @Test
    public void testServerSignatureDiffer() throws Exception {
        Mechanism mechanism = getMechanismForTesting();

        mechanism.getInitialResponse(getTestCredentials());
        mechanism.getChallengeResponse(getTestCredentials(), serverFirstMessage);

        ProtonBuffer challenge = ProtonBufferAllocator.defaultAllocator().copy("v=badserverfinal".getBytes());
        challenge.setWriteOffset(challenge.capacity());

        try {
            mechanism.getChallengeResponse(getTestCredentials(), challenge);
            fail("Exception not thrown");
        } catch (SaslException e) {
            // PASS
        }
    }

    @Test
    public void testIncompleteExchange() throws Exception {
        Mechanism mechanism = getMechanismForTesting();

        ProtonBuffer clientInitialResponse = mechanism.getInitialResponse(getTestCredentials());
        assertEquals(expectedClientInitialResponse, clientInitialResponse);

        ProtonBuffer clientFinalMessage = mechanism.getChallengeResponse(getTestCredentials(), serverFirstMessage);
        assertEquals(expectedClientFinalMessage, clientFinalMessage);

        try {
            mechanism.verifyCompletion();
            fail("Exception not thrown");
        } catch (SaslException e) {
            // PASS
        }
    }
}