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
 * The known good used by these tests is taken from the example in RFC 7677 section 3.
 */
public class ScramSHA512MechanismTest extends AbstractScramSHAMechanismTestBase {

    private static final String TEST_USERNAME = "user";
    private static final String TEST_PASSWORD = "pencil";

    private static final String CLIENT_NONCE = "rOprNGfwEbeRWgbNEkqO";

    private static final ProtonBuffer EXPECTED_CLIENT_INITIAL_RESPONSE = ProtonByteBufferAllocator.DEFAULT.wrap(
        "n,,n=user,r=rOprNGfwEbeRWgbNEkqO".getBytes(StandardCharsets.UTF_8));
    private static final ProtonBuffer SERVER_FIRST_MESSAGE = ProtonByteBufferAllocator.DEFAULT.wrap(
        "r=rOprNGfwEbeRWgbNEkqO02431b08-2f89-4bad-a4e6-80c0564ec865,s=Yin2FuHTt/M0kJWb0t9OI32n2VmOGi3m+JfjOvuDF88=,i=4096".getBytes(StandardCharsets.UTF_8));
    private static final ProtonBuffer EXPECTED_CLIENT_FINAL_MESSAGE = ProtonByteBufferAllocator.DEFAULT.wrap(
        "c=biws,r=rOprNGfwEbeRWgbNEkqO02431b08-2f89-4bad-a4e6-80c0564ec865,p=Hc5yec3NmCD7t+kFRw4/3yD6/F3SQHc7AVYschRja+Bc3sbdjlA0eH1OjJc0DD4ghn1tnXN5/Wr6qm9xmaHt4A==".getBytes(StandardCharsets.UTF_8));
    private static final ProtonBuffer SERVER_FINAL_MESSAGE = ProtonByteBufferAllocator.DEFAULT.wrap(
        "v=BQuhnKHqYDwQWS5jAw4sZed+C9KFUALsbrq81bB0mh+bcUUbbMPNNmBIupnS2AmyyDnG5CTBQtkjJ9kyY4kzmw==".getBytes(StandardCharsets.UTF_8));

    public ScramSHA512MechanismTest() {
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
        return new ScramSHA512Mechanism(CLIENT_NONCE);
    }

    @Test
    public void testGetNameMatchesValueInSaslMechanismsEnum() {
        assertEquals(SaslMechanisms.SCRAM_SHA_512.getName(), getMechanismForTesting().getName());
    }

    @Test
    public void testDifferentClientNonceOnEachInstance() throws Exception {
        ScramSHA512Mechanism mech1 = new ScramSHA512Mechanism();
        ScramSHA512Mechanism mech2 = new ScramSHA512Mechanism();

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

        ScramSHA512Mechanism mech = new ScramSHA512Mechanism(CLIENT_NONCE);

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
            "r=rOprNGfwEbeRWgbNEkqOf0f492bc-13cc-4050-8461-59f74f24e989,s=g2nOdJkyb5SlvqLbJb6S5+ckZpYFJ+AkJqxlmDAZYbY=,i=4096".getBytes(StandardCharsets.UTF_8));
        ProtonBuffer expectedClientFinalMessage = ProtonByteBufferAllocator.DEFAULT.wrap(
            "c=biws,r=rOprNGfwEbeRWgbNEkqOf0f492bc-13cc-4050-8461-59f74f24e989,p=vxWDY/qwIhNPGnYvGKxRESmP9nP4bmOSssNLVN6sWo1cAatr3HAxIogJ9qe2kxLdrmQcyCkW7sgq+8ybSgPphQ==".getBytes(StandardCharsets.UTF_8));

        ProtonBuffer clientFinalMessage = mechanism.getChallengeResponse(credentials, serverFirstMessage);

        assertEquals(expectedClientFinalMessage, clientFinalMessage);

        ProtonBuffer serverFinalMessage = ProtonByteBufferAllocator.DEFAULT.wrap(
            "v=l/icAMt3q4ym4Yh7syjjekFZ3r3L3+l+e08WmS3m3pMXCXhPf865+9bfRRprO6xPhFWKyuD+PPh+jQf8JBVojQ==".getBytes(StandardCharsets.UTF_8));
        ProtonBuffer expectedFinalChallengeResponse = ProtonByteBufferAllocator.DEFAULT.wrap("".getBytes());

        assertEquals(expectedFinalChallengeResponse, mechanism.getChallengeResponse(credentials, serverFinalMessage));

        mechanism.verifyCompletion();
    }
}