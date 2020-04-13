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
package org.apache.qpid.proton4j.engine.sasl.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.security.sasl.SaslException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.junit.Test;

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
        assertTrue("Should be applicable with no credentials",
            SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials(null, null, false)));
    }

    @Test
    public void testIsNotApplicableWithNoUser() {
        assertTrue("Should be applicable with no username",
            SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials(null, "pass", false)));
    }

    @Test
    public void testIsApplicableWithNoPassword() {
        assertTrue("Should be applicable with no password",
            SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("user", null, false)));
    }

    @Test
    public void testIsApplicableWithEmtpyUser() {
        assertTrue("Should be applicable with empty username",
            SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("", "pass", false)));
    }

    @Test
    public void testIsApplicableWithEmtpyPassword() {
        assertTrue("Should be applicable with empty password",
            SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("user", "", false)));
    }

    @Test
    public void testIsApplicableWithEmtpyUserAndPassword() {
        assertTrue("Should be applicable with empty user and password",
            SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("", "", false)));
    }

    @Test
    public void testIsApplicableWithUserAndPassword() {
        assertTrue("Should be applicable with user and password",
            SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("user", "password", false)));
    }

    @Test
    public void testIsApplicableWithUserAndPasswordAndPrincipal() {
        assertTrue("Should be applicable with user and password and principal",
            SaslMechanisms.ANONYMOUS.createMechanism().isApplicable(credentials("user", "password", true)));
    }
}
