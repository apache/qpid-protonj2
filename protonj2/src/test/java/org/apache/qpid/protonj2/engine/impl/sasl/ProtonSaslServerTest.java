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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.engine.Engine;
import org.apache.qpid.protonj2.engine.EngineFactory;
import org.apache.qpid.protonj2.engine.exceptions.ProtocolViolationException;
import org.apache.qpid.protonj2.engine.impl.ProtonEngineTestSupport;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.apache.qpid.protonj2.engine.sasl.SaslServerContext;
import org.apache.qpid.protonj2.engine.sasl.SaslServerListener;
import org.apache.qpid.protonj2.test.driver.ProtonTestConnector;
import org.apache.qpid.protonj2.test.driver.codec.security.SaslCode;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AMQPHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test proton engine from the perspective of a SASL client
 */
@Timeout(20)
public class ProtonSaslServerTest extends ProtonEngineTestSupport {

    @Test
    public void testEngineFailsIfAMQPHeaderArrivesWhenSASLHeaderExpected() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Setup basic SASL server which only allows ANONYMOUS
        engine.saslDriver().server().setListener(createAnonymousSaslServerListener());
        engine.connection().openHandler((conn) -> conn.open());
        engine.connection().closeHandler((conn) -> conn.close());
        engine.start();

        peer.expectSASLHeader();

        try {
            peer.remoteHeader(AMQPHeader.getAMQPHeader().toArray()).now();
        } catch (AssertionError pve) {
            assertTrue(pve.getCause() instanceof ProtocolViolationException);
        }

        peer.waitForScriptToCompleteIgnoreErrors();

        assertNotNull(failure);
    }

    @Test
    public void testSaslAnonymousConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Setup basic SASL server which only allows ANONYMOUS
        engine.saslDriver().server().setListener(createAnonymousSaslServerListener());
        engine.connection().openHandler((conn) -> conn.open());
        engine.connection().closeHandler((conn) -> conn.close());
        engine.start();

        peer.expectSASLHeader();
        peer.expectSaslMechanisms().withSaslServerMechanisms("ANONYMOUS");
        peer.remoteHeader(AMQPHeader.getSASLHeader().toArray()).now();
        peer.waitForScriptToComplete();

        peer.expectSaslOutcome().withCode(SaslCode.OK);
        peer.remoteSaslInit().withMechanism("ANONYMOUS").now();
        peer.waitForScriptToComplete();

        peer.expectAMQPHeader();
        peer.expectOpen();
        peer.remoteHeader(AMQPHeader.getAMQPHeader().toArray()).now();
        peer.remoteOpen().now();
        peer.waitForScriptToComplete();

        peer.expectClose();
        peer.remoteClose().now();
        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSaslPlainConnection() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Setup basic SASL server which only allows ANONYMOUS
        engine.saslDriver().server().setListener(createPlainSaslServerListener());
        engine.connection().openHandler((conn) -> conn.open());
        engine.connection().closeHandler((conn) -> conn.close());
        engine.start();

        peer.expectSASLHeader();
        peer.expectSaslMechanisms().withSaslServerMechanisms("PLAIN");
        peer.remoteHeader(AMQPHeader.getSASLHeader().toArray()).now();
        peer.waitForScriptToComplete();

        peer.expectSaslOutcome().withCode(SaslCode.OK);
        peer.remoteSaslInit().withMechanism("PLAIN")
                             .withInitialResponse(saslPlainInitialResponse("user", "pass")).now();
        peer.waitForScriptToComplete();

        peer.expectAMQPHeader();
        peer.expectOpen();
        peer.remoteHeader(AMQPHeader.getAMQPHeader().toArray()).now();
        peer.remoteOpen().now();
        peer.waitForScriptToComplete();

        peer.expectClose();
        peer.remoteClose().now();
        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testSaslPlainConnectionFailedWhenAnonymousOffered() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Setup basic SASL server which only allows ANONYMOUS
        engine.saslDriver().server().setListener(createPlainSaslServerListener());
        engine.connection().openHandler((conn) -> conn.open());
        engine.connection().closeHandler((conn) -> conn.close());
        engine.start();

        peer.expectSASLHeader();
        peer.expectSaslMechanisms().withSaslServerMechanisms("PLAIN");
        peer.remoteHeader(AMQPHeader.getSASLHeader().toArray()).now();
        peer.waitForScriptToComplete();

        peer.expectSaslOutcome().withCode(SaslCode.SYS_PERM);
        peer.remoteSaslInit().withMechanism("ANONYMOUS").now();
        peer.waitForScriptToComplete();

        assertNull(failure);
    }

    @Test
    public void testEngineFailsForUnexpecetedNonSaslFrameDuringSaslExchange() throws Exception {
        Engine engine = EngineFactory.PROTON.createEngine();
        engine.errorHandler(result -> failure = result.failureCause());
        ProtonTestConnector peer = createTestPeer(engine);

        // Setup basic SASL server which only allows ANONYMOUS
        engine.saslDriver().server().setListener(createPlainSaslServerListener());
        engine.connection().openHandler((conn) -> conn.open());
        engine.connection().closeHandler((conn) -> conn.close());
        engine.start();

        peer.expectSASLHeader();
        peer.expectSaslMechanisms().withSaslServerMechanisms("PLAIN");
        peer.remoteHeader(AMQPHeader.getSASLHeader().toArray()).now();
        peer.waitForScriptToComplete();

        try {
            peer.remoteOpen().now();
        } catch (AssertionError pve) {
            assertTrue(pve.getCause() instanceof ProtocolViolationException);
        }

        peer.waitForScriptToCompleteIgnoreErrors();

        assertNotNull(failure);
    }

    public byte[] saslPlainInitialResponse(String username, String password) {
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
        byte[] initialResponse = new byte[usernameBytes.length + passwordBytes.length + 2];
        System.arraycopy(usernameBytes, 0, initialResponse, 1, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, initialResponse, 2 + usernameBytes.length, passwordBytes.length);

        return initialResponse;
    }

    private SaslServerListener createAnonymousSaslServerListener() {
        return new SaslServerListener() {

            @Override
            public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {
                throw new RuntimeException("Not expecting any SASL Response frames");
            }

            @Override
            public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
                if (mechanism.equals(Symbol.valueOf("ANONYMOUS"))) {
                    context.sendOutcome(SaslOutcome.SASL_OK, null);
                } else {
                    context.sendOutcome(SaslOutcome.SASL_PERM, null);
                }
            }

            @Override
            public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
                context.sendMechanisms(new Symbol[] { Symbol.valueOf("ANONYMOUS") });
            }
        };
    }

    private SaslServerListener createPlainSaslServerListener() {
        return new SaslServerListener() {

            @Override
            public void handleSaslResponse(SaslServerContext context, ProtonBuffer response) {
                throw new RuntimeException("Not expecting any SASL Response frames");
            }

            @Override
            public void handleSaslInit(SaslServerContext context, Symbol mechanism, ProtonBuffer initResponse) {
                if (mechanism.equals(Symbol.valueOf("PLAIN"))) {
                    context.sendOutcome(SaslOutcome.SASL_OK, null);
                } else {
                    context.sendOutcome(SaslOutcome.SASL_PERM, null);
                }
            }

            @Override
            public void handleSaslHeader(SaslServerContext context, AMQPHeader header) {
                context.sendMechanisms(new Symbol[] { Symbol.valueOf("PLAIN") });
            }
        };
    }
}
