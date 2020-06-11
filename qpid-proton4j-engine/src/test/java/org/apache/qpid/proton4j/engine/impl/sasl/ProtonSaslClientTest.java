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
package org.apache.qpid.proton4j.engine.impl.sasl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.security.Principal;

import javax.security.sasl.SaslException;

import org.apache.qpid.proton4j.amqp.driver.ProtonTestPeer;
import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineFactory;
import org.apache.qpid.proton4j.engine.impl.ProtonEngineTestSupport;
import org.apache.qpid.proton4j.engine.sasl.client.SaslAuthenticator;
import org.apache.qpid.proton4j.engine.sasl.client.SaslCredentialsProvider;
import org.apache.qpid.proton4j.types.security.SaslCode;
import org.junit.Test;

/**
 * Test proton engine from the perspective of a SASL client
 */
public class ProtonSaslClientTest extends ProtonEngineTestSupport {

    @Test
    public void testSaslAnonymousConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

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
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectSASLHeader().respondWithSASLPHeader();
        peer.remoteSaslMechanisms().withMechanisms("PLAIN", "ANONYMOUS").queue();
        peer.expectSaslInit().withMechanism("ANONYMOUS");
        peer.remoteSaslOutcome().withCode(SaslCode.OK).queue();
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
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

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
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        // Expect a PLAIN connection
        String user = "user";
        String pass = "qwerty123456";

        peer.expectSASLHeader().respondWithSASLPHeader();
        peer.remoteSaslMechanisms().withMechanisms("UNKNOWN", "PLAIN", "ANONYMOUS").queue();
        peer.expectSaslInit().withMechanism("PLAIN").withInitialResponse(peer.saslPlainInitialResponse(user, pass));
        peer.remoteSaslOutcome().withCode(SaslCode.OK).queue();
        peer.expectAMQPHeader().respondWithAMQPHeader();

        peer.expectOpen().respond();
        peer.expectClose().respond();

        engine.saslDriver().client().setListener(createSaslPlainAuthenticator(user, pass));

        Connection connection = engine.start().open();

        connection.close();

        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test(timeout = 20000)
    public void testSaslXOauth2Connection() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

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
        engine.errorHandler(result -> failure = result);
        ProtonTestPeer peer = new ProtonTestPeer(engine);
        engine.outputConsumer(peer);

        peer.expectSASLHeader().respondWithSASLPHeader();
        peer.remoteSaslMechanisms().withMechanisms("PLAIN", "ANONYMOUS").queue();
        peer.expectSaslInit().withMechanism("PLAIN");
        peer.remoteSaslOutcome().withCode(saslFailureCode).queue();

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
