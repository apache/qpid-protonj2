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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.junit.jupiter.api.Test;

public class OauthBearerMechanismTest extends MechanismTestBase {

    @Test
    public void testGetInitialResponseWithNullUserAndPassword() throws SaslException {
        OauthBearerMechanism mech = new OauthBearerMechanism();

        ProtonBuffer response = mech.getInitialResponse(secureCredentials());
        assertNotNull(response);
        assertTrue(response.getReadableBytes() != 0);
    }

    @Test
    public void testGetChallengeResponse() throws SaslException {
        OauthBearerMechanism mech = new OauthBearerMechanism();

        ProtonBuffer response = mech.getChallengeResponse(secureCredentials(), TEST_BUFFER);
        assertNotNull(response);
        assertTrue(response.getReadableBytes() == 0);
    }

    @Test
    public void testIsNotApplicableWithInsecureCredentials() {
        assertFalse(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(credentials("user", "token", false)),
            "Should not be applicable with no credentials");
    }

    @Test
    public void testIsNotApplicableWithNoCredentials() {
        assertFalse(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(secureCredentials(null, null, false)),
            "Should not be applicable with no credentials");
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        assertFalse(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(secureCredentials(null, "pass", false)),
            "Should not be applicable with no username");
    }

    @Test
    public void testIsNotApplicableWithNoToken() {
        assertFalse(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(secureCredentials("user", null, false)),
            "Should not be applicable with no token");
    }

    @Test
    public void testIsNotApplicableWithEmtpyUser() {
        assertFalse(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(secureCredentials("", "pass", false)),
            "Should not be applicable with empty username");
    }

    @Test
    public void testIsNotApplicableWithEmtpyToken() {
        assertFalse(SaslMechanisms.XOAUTH2.createMechanism().isApplicable(secureCredentials("user", "", false)),
            "Should not be applicable with empty token");
    }

    @Test
    public void testIsNotApplicableWithIllegalAccessToken() {
        assertFalse(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(secureCredentials("user", "illegalChar\000", false)),
            "Should not be applicable with non vschars");
    }

    @Test
    public void testIsNotApplicableWithEmtpyUserAndToken() {
        assertFalse(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(secureCredentials("", "", false)),
            "Should not be applicable with empty user and token");
    }

    @Test
    public void testIsApplicableWithUserAndToken() {
        assertTrue(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(secureCredentials("user", "2YotnFZFEjr1zCsicMWpAA", false)),
            "Should be applicable with user and token");
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        assertTrue(SaslMechanisms.OAUTHBEARER.createMechanism().isApplicable(secureCredentials("user", "2YotnFZFEjr1zCsicMWpAA", true)),
            "Should be applicable with user and token and principal");
    }
}
