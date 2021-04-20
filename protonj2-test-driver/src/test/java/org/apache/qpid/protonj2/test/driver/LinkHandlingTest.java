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

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.utils.TestPeerTestsBase;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LinkHandlingTest extends TestPeerTestsBase {

    private static final Logger LOG = LoggerFactory.getLogger(LinkHandlingTest.class);

    @Test
    public void testClientToServerSenderAttach() throws Exception {
        try (ProtonTestServer peer = new ProtonTestServer();
            ProtonTestClient client = new ProtonTestClient()) {

            peer.expectAMQPHeader().respondWithAMQPHeader();
            peer.expectOpen().respond();
            peer.expectBegin().onChannel(0).respond();
            peer.expectAttach().ofSender().onChannel(0).respond();
            peer.expectDetach().onChannel(0).respond();
            peer.expectEnd().onChannel(0).respond();
            peer.expectClose().respond();
            peer.start();

            URI remoteURI = peer.getServerURI();
            LOG.info("Test started, peer listening on: {}", remoteURI);

            client.expectAMQPHeader();
            client.expectOpen();
            client.expectBegin().onChannel(0);
            client.expectAttach().ofReceiver().withHandle(0);
            client.expectDetach().withHandle(0);
            client.expectEnd();
            client.expectClose();
            client.connect(remoteURI.getHost(), remoteURI.getPort());

            // This initiates the tests and waits for proper completion.
            client.remoteHeader(AMQPHeader.getAMQPHeader()).now();
            client.remoteOpen().now();
            client.remoteBegin().now();
            client.remoteAttach().ofSender().now();
            client.remoteDetach().now();
            client.remoteEnd().now();
            client.remoteClose().now();

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

            client.waitForScriptToComplete(5, TimeUnit.SECONDS);
        }
    }
}
