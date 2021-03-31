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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.junit.jupiter.api.Test;

/**
 * The known good used by these tests is taken from the example in RFC 5802 section 5.
 */
public class ScramSHA1MechanismTest extends AbstractScramSHAMechanismTestBase {

    private static final String TEST_USERNAME = "user";
    private static final String TEST_PASSWORD = "pencil";

    private static final String CLIENT_NONCE = "fyko+d2lbbFgONRv9qkxdawL";

    private static final ProtonBuffer EXPECTED_CLIENT_INITIAL_RESPONSE = ProtonByteBufferAllocator.DEFAULT.wrap(
        "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8));
    private static final ProtonBuffer SERVER_FIRST_MESSAGE = ProtonByteBufferAllocator.DEFAULT.wrap(
        "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".getBytes(StandardCharsets.UTF_8));
    private static final ProtonBuffer EXPECTED_CLIENT_FINAL_MESSAGE = ProtonByteBufferAllocator.DEFAULT.wrap(
        "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=".getBytes(StandardCharsets.UTF_8));
    private static final ProtonBuffer SERVER_FINAL_MESSAGE = ProtonByteBufferAllocator.DEFAULT.wrap(
        "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=".getBytes(StandardCharsets.UTF_8));

    public ScramSHA1MechanismTest() {
        super(EXPECTED_CLIENT_INITIAL_RESPONSE,
              SERVER_FIRST_MESSAGE,
              EXPECTED_CLIENT_FINAL_MESSAGE,
              SERVER_FINAL_MESSAGE);
    }

    @Override
    protected SaslCredentialsProvider getTestCredentials() {
        return credentials(TEST_USERNAME, TEST_PASSWORD);
    }

    @Override
    protected Mechanism getMechanismForTesting() {
        return new ScramSHA1Mechanism(CLIENT_NONCE);
    }

    @Test
    public void testGetNameMatchesValueInSaslMechanismsEnum() {
        assertEquals(SaslMechanisms.SCRAM_SHA_1.getName(), getMechanismForTesting().getName());
    }

    @Test
    public void testDifferentClientNonceOnEachInstance() throws Exception {
        ScramSHA1Mechanism mech1 = new ScramSHA1Mechanism();
        ScramSHA1Mechanism mech2 = new ScramSHA1Mechanism();

        ProtonBuffer clientInitialResponse1 = mech1.getInitialResponse(getTestCredentials());
        ProtonBuffer clientInitialResponse2 = mech2.getInitialResponse(getTestCredentials());

        assertTrue(clientInitialResponse1.toString(StandardCharsets.UTF_8).startsWith("n,,n=user,r="));
        assertTrue(clientInitialResponse2.toString(StandardCharsets.UTF_8).startsWith("n,,n=user,r="));

        assertThat(clientInitialResponse1, not(equalTo(clientInitialResponse2)));
    }

    @Test
    public void testUsernameCommaEqualsCharactersEscaped() throws Exception {
        String originalUsername = "user,name=";
        String escapedUsername = "user=2Cname=3D";

        String expectedInitialResponseString = "n,,n=" + escapedUsername + ",r=" + CLIENT_NONCE;
        ProtonBuffer expectedInitialResponseBuffer = ProtonByteBufferAllocator.DEFAULT.wrap(
            expectedInitialResponseString.getBytes(StandardCharsets.UTF_8));

        ScramSHA1Mechanism mech = new ScramSHA1Mechanism(CLIENT_NONCE);

        ProtonBuffer clientInitialResponse = mech.getInitialResponse(credentials(originalUsername, "password"));
        assertEquals(expectedInitialResponseBuffer, clientInitialResponse);
    }

    @Test
    public void testPasswordCommaEqualsCharactersNotEscaped() throws Exception {
        Mechanism mechanism = getMechanismForTesting();
        SaslCredentialsProvider credentials = credentials(TEST_USERNAME, TEST_PASSWORD + ",=");

        ProtonBuffer clientInitialResponse = mechanism.getInitialResponse(credentials);
        assertEquals(EXPECTED_CLIENT_INITIAL_RESPONSE, clientInitialResponse);

        ProtonBuffer serverFirstMessage = ProtonByteBufferAllocator.DEFAULT.wrap(
            "r=fyko+d2lbbFgONRv9qkxdawLdcbfa301-1618-46ee-96c1-2bf60139dc7f,s=Q0zM1qzKMOmI0sAzE7dXt6ru4ZIXhAzn40g4mQXKQdw=,i=4096".getBytes(StandardCharsets.UTF_8));
        ProtonBuffer expectedClientFinalMessage = ProtonByteBufferAllocator.DEFAULT.wrap(
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawLdcbfa301-1618-46ee-96c1-2bf60139dc7f,p=quRNWvZqGUvPXoazebZe0ZYsjQI=".getBytes(StandardCharsets.UTF_8));

        ProtonBuffer clientFinalMessage = mechanism.getChallengeResponse(credentials, serverFirstMessage);

        assertEquals(expectedClientFinalMessage, clientFinalMessage);

        ProtonBuffer serverFinalMessage = ProtonByteBufferAllocator.DEFAULT.wrap(
            "v=dnJDHm3fp6WwVrl5yjZuqKp03lQ=".getBytes(StandardCharsets.UTF_8));
        ProtonBuffer expectedFinalChallengeResponse = ProtonByteBufferAllocator.DEFAULT.wrap("".getBytes());

        assertEquals(expectedFinalChallengeResponse, mechanism.getChallengeResponse(credentials, serverFinalMessage));

        mechanism.verifyCompletion();
    }

}