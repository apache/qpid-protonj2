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
package org.apache.qpid.protonj2.engine.impl.sasl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.Principal;

import javax.security.sasl.SaslException;

import org.apache.qpid.protonj2.engine.Connection;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.impl.ProtonEngineTestSupport;
import org.apache.qpid.protonj2.engine.sasl.client.SaslAuthenticator;
import org.apache.qpid.protonj2.engine.sasl.client.SaslCredentialsProvider;
import org.apache.qpid.protonj2.test.driver.ProtonTestPeer;
import org.apache.qpid.protonj2.types.security.SaslCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test proton engine from the perspective of a SASL client
 */
@Timeout(20)
public class ProtonSaslClientTest extends ProtonEngineTestSupport {

    @Test
    public void testSaslAnonymousConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectSASLAnonymousConnect();
        peer.expectOpen().respond();
        peer.expectClose().respond();

        engine.saslDriver().client().setListener(createSaslPlainAuthenticator(null, null));

        Connection connection = engine.start().open();

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSaslAnonymousConnectionWhenPlainAlsoOfferedButNoCredentialsGiven() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectSASLHeader().respondWithSASLPHeader();
        peer.remoteSaslMechanisms().withMechanisms("PLAIN", "ANONYMOUS").queue();
        peer.expectSaslInit().withMechanism("ANONYMOUS");
        peer.remoteSaslOutcome().withCode(SaslCode.OK.byteValue()).queue();
        peer.expectAMQPHeader().respondWithAMQPHeader();
        peer.expectOpen().respond();
        peer.expectClose().respond();

        engine.saslDriver().client().setListener(createSaslPlainAuthenticator(null, null));

        Connection connection = engine.start().open();

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSaslPlainConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestPeer peer = createTestPeer(engine);

        // Expect a PLAIN connection
        String user = "user";
        String pass = "qwerty123456";

        peer.expectSASLPlainConnect(user, pass);
        peer.expectOpen().respond();
        peer.expectClose().respond();

        engine.saslDriver().client().setListener(createSaslPlainAuthenticator(user, pass));

        Connection connection = engine.start().open();

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSaslPlainConnectionWhenUnknownMechanismsOfferedBeforeIt() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestPeer peer = createTestPeer(engine);

        // Expect a PLAIN connection
        String user = "user";
        String pass = "qwerty123456";

        peer.expectSASLHeader().respondWithSASLPHeader();
        peer.remoteSaslMechanisms().withMechanisms("UNKNOWN", "PLAIN", "ANONYMOUS").queue();
        peer.expectSaslInit().withMechanism("PLAIN").withInitialResponse(peer.saslPlainInitialResponse(user, pass));
        peer.remoteSaslOutcome().withCode(SaslCode.OK.byteValue()).queue();
        peer.expectAMQPHeader().respondWithAMQPHeader();

        peer.expectOpen().respond();
        peer.expectClose().respond();

        engine.saslDriver().client().setListener(createSaslPlainAuthenticator(user, pass));

        Connection connection = engine.start().open();

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSaslXOauth2Connection() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestPeer peer = createTestPeer(engine);

        // Expect a XOAUTH2 connection
        String user = "user";
        String pass = "eyB1c2VyPSJ1c2VyIiB9";

        peer.expectSaslXOauth2Connect(user, pass);
        peer.expectOpen().respond();
        peer.expectClose().respond();

        engine.saslDriver().client().setListener(createSaslPlainAuthenticator(user, pass));

        Connection connection = engine.start().open();

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSaslFailureCodesFailEngine() throws Exception {
        doSaslFailureCodesTestImpl(SaslCode.AUTH);
        doSaslFailureCodesTestImpl(SaslCode.SYS);
        doSaslFailureCodesTestImpl(SaslCode.SYS_PERM);
        doSaslFailureCodesTestImpl(SaslCode.SYS_TEMP);
    }

    private void doSaslFailureCodesTestImpl(SaslCode saslFailureCode) throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestPeer peer = createTestPeer(engine);

        peer.expectSASLHeader().respondWithSASLPHeader();
        peer.remoteSaslMechanisms().withMechanisms("PLAIN", "ANONYMOUS").queue();
        peer.expectSaslInit().withMechanism("PLAIN");
        peer.remoteSaslOutcome().withCode(saslFailureCode.byteValue()).queue();

        engine.saslDriver().client().setListener(createSaslPlainAuthenticator("user", "pass"));

        engine.start().open();

        peer.waitForScriptToComplete();

        assertNotNull(failure);
        assertFalse(engine.isShutdown());
        assertTrue(engine.isFailed());
        assertEquals(failure, engine.failureCause());
        assertTrue(failure instanceof SaslException);
    }

    private SaslAuthenticator createSaslPlainAuthenticator(String user, String password) {
        SaslCredentialsProvider credentials = new SaslCredentialsProvider() {

            @Override
            public String vhost() {
                return null;
            }

            @Override
            public String username() {
                return user;
            }

            @Override
            public String password() {
                return password;
            }

            @Override
            public Principal localPrincipal() {
                return null;
            }
        };

        return new SaslAuthenticator(credentials);
    }
}
