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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Base64;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.junit.jupiter.api.Test;

/**
 * The known good used by these tests is taken from the example in RFC 2195 section 2.
 */
public class CramMD5MechanismTest extends MechanismTestBase {

    private final ProtonBuffer SERVER_FIRST_MESSAGE = ProtonBufferAllocator.defaultAllocator().copy(
        Base64.getDecoder().decode("PDE4OTYuNjk3MTcwOTUyQHBvc3RvZmZpY2UucmVzdG9uLm1jaS5uZXQ+")).convertToReadOnly();

    private final ProtonBuffer EXPECTED_CLIENT_FINAL_MESSAGE = ProtonBufferAllocator.defaultAllocator().copy(
        Base64.getDecoder().decode("dGltIGI5MTNhNjAyYzdlZGE3YTQ5NWI0ZTZlNzMzNGQzODkw")).convertToReadOnly();

    private static final String TEST_USERNAME = "tim";
    private static final String TEST_PASSWORD = "tanstaaftanstaaf";

    @Test
    public void testSuccessfulAuthentication() throws Exception {
        Mechanism mechanism = new CramMD5Mechanism();
        SaslCredentialsProvider creds = credentials(TEST_USERNAME, TEST_PASSWORD);

        ProtonBuffer clientInitialResponse = mechanism.getInitialResponse(creds);
        assertNull(clientInitialResponse);

        ProtonBuffer clientFinalResponse = mechanism.getChallengeResponse(creds, SERVER_FIRST_MESSAGE);
        assertEquals(EXPECTED_CLIENT_FINAL_MESSAGE, clientFinalResponse);

        mechanism.verifyCompletion();
    }

    @Test
    public void testIsNotApplicableWithNoCredentials() {
        assertFalse(SaslMechanisms.CRAM_MD5.createMechanism().isApplicable(credentials(null, null, false)),
            "Should not be applicable with no credentials");
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        assertFalse(SaslMechanisms.CRAM_MD5.createMechanism().isApplicable(credentials(null, "pass", false)),
            "Should not be applicable with no username");
    }

    @Test
    public void testIsNotApplicableWithNoPassword() {
        assertFalse(SaslMechanisms.CRAM_MD5.createMechanism().isApplicable(credentials("user", null, false)),
            "Should not be applicable with no password");
    }

    @Test
    public void testIsNotApplicableWithEmptyUser() {
        assertFalse(SaslMechanisms.CRAM_MD5.createMechanism().isApplicable(credentials("", "pass", false)),
            "Should not be applicable with empty username");
    }

    @Test
    public void testIsNotApplicableWithEmptyPassword() {
        assertFalse(SaslMechanisms.CRAM_MD5.createMechanism().isApplicable(credentials("user", "", false)),
            "Should not be applicable with empty password");
    }

    @Test
    public void testIsNotApplicableWithEmptyUserAndPassword() {
        assertFalse(SaslMechanisms.CRAM_MD5.createMechanism().isApplicable(credentials("", "", false)),
            "Should not be applicable with empty user and password");
    }

    @Test
    public void testIsApplicableWithUserAndPassword() {
        assertTrue(SaslMechanisms.CRAM_MD5.createMechanism().isApplicable(credentials("user", "pass", false)),
            "Should be applicable with user and password");
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        assertTrue(SaslMechanisms.CRAM_MD5.createMechanism().isApplicable(credentials("user", "pass", true)),
            "Should be applicable with user and password and principal");
    }

    @Test
    public void testIncompleteExchange() throws Exception {
        Mechanism mechanism = new CramMD5Mechanism();

        mechanism.getInitialResponse(credentials(TEST_USERNAME, TEST_PASSWORD));

        try {
            mechanism.verifyCompletion();
            fail("Exception not thrown");
        } catch (SaslException e) {
            // PASS
        }
    }
}
