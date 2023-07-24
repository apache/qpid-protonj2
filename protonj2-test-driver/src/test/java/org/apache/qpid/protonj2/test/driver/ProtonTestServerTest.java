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
package org.apache.qpid.protonj2.test.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.test.driver.codec.security.SaslCode;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.utils.TestPeerTestsBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
public class ProtonTestServerTest extends TestPeerTestsBase {

    @Test
    public void testServerStart() throws Exception {
        ProtonTestServer peer = new ProtonTestServer();

        assertFalse(peer.isClosed());
        peer.start();
        peer.close();
        assertTrue(peer.isClosed());
    }

    @Test
    public void testSupplyQueryStringToServerGetURI() throws Exception {
        doTestSupplyQueryStringToServerGetURI("param=value");
    }

    @Test
    public void testSupplyQueryStringToServerGetURIWithLeadingQuestionMark() throws Exception {
        doTestSupplyQueryStringToServerGetURI("?param=value");
    }

    @Test
    public void testSupplyNullQueryStringToServerGetURI() throws Exception {
        doTestSupplyQueryStringToServerGetURI(null);
    }

    private void doTestSupplyQueryStringToServerGetURI(String queryString) throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().withFrameSize(4096);
            peer.start();

            final URI remoteURI = peer.getServerURI(queryString);

            if (queryString != null && queryString.startsWith("?")) {
                queryString = queryString.substring(1);
            }

            assertEquals(queryString, remoteURI.getQuery());
        }
    }

    @Test
    public void testServerFailsTestIfFrameSizeExpectationNotMet() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().withFrameSize(4096);
            peer.start();

            URI remoteURI = peer.getServerURI();

            ProtonTestClient client = new ProtonTestClient();

            client.connect(remoteURI.getHost(), remoteURI.getPort());
            client.expectAMQPHeader();
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();

            assertThrows(AssertionError.class, () -> peer.waitForScriptToComplete(5, TimeUnit.SECONDS));

            client.close();
        }
    }

    @Test
    public void testServerExpectsSaslPlainConnectOffersMoreThanPlain() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer()) {
            peer.expectSASLPlainConnect("user", "pass", "EXTERNAL", "PLAIN", "ANONYMOUS");
            peer.start();

            URI remoteURI = peer.getServerURI();

            try (ProtonTestClient client = new ProtonTestClient()) {
                client.connect(remoteURI.getHost(), remoteURI.getPort());
                client.expectSASLHeader();
                client.expectSaslMechanisms().withSaslServerMechanisms("EXTERNAL", "PLAIN", "ANONYMOUS");
                client.remoteSaslInit().withMechanism("PLAIN").withInitialResponse(peer.saslPlainInitialResponse("user", "pass")).queue();
                client.expectSaslOutcome().withCode(SaslCode.OK);
                client.remoteHeader(AMQPHeader.getAMQPHeader()).queue();
                client.expectAMQPHeader();

                // Start the exchange with the client SASL header
                client.remoteHeader(AMQPHeader.getSASLHeader()).now();

                client.waitForScriptToComplete(5, TimeUnit.SECONDS);
            }

            peer.waitForScriptToComplete();
        }
    }
}
