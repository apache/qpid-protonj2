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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.junit.jupiter.api.Test;

public class AnonymousMechanismTest extends MechanismTestBase {

    @Test
    public void testGetInitialResponseWithNullUserAndPassword() throws SaslException {
        AnonymousMechanism mech = new AnonymousMechanism();

        ProtonBuffer response = mech.getInitialResponse(credentials());
        assertNotNull(response);
        assertTrue(response.getReadableBytes() == 0);
    }

    @Test
    public void testGetChallengeResponse() throws SaslException {
        AnonymousMechanism mech = new AnonymousMechanism();

        ProtonBuffer response = mech.getChallengeResponse(credentials(), TEST_BUFFER);
        assertNotNull(response);
        assertTrue(response.getReadableBytes() == 0);
    }

    @Test
    public void testIsApplicableWithNoCredentials() {
        assertTrue(SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials(null, null, false)),
            "Should be applicable with no credentials");
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        assertTrue(SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials(null, "pass", false)),
            "Should be applicable with no username");
    }

    @Test
    public void testIsApplicableWithNoPassword() {
        assertTrue(SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("user", null, false)),
            "Should be applicable with no password");
    }

    @Test
    public void testIsApplicableWithEmtpyUser() {
        assertTrue(SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("", "pass", false)),
            "Should be applicable with  username");
    }

    @Test
    public void testIsApplicableWithEmtpyPassword() {
        assertTrue(SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("user", "", false)),
            "Should be applicable with  password");
    }

    @Test
    public void testIsApplicableWithEmtpyUserAndPassword() {
        assertTrue(SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("", "", false)),
            "Should be applicable with  user and password");
    }

    @Test
    public void testIsApplicableWithUserAndPassword() {
        assertTrue(SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("user", "password", false)),
            "Should be applicable with user and password");
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        assertTrue(SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("user", "password", true)),
            "Should be applicable with user and password and principal");
    }
}
