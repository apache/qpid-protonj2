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
package org.apache.qpid.protonj2.client.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.exceptions.ClientClosedException;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.apache.qpid.protonj2.client.test.ImperativeClientTestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test the Client API implementation
 */
@Timeout(20)
public class ClientTest extends ImperativeClientTestCase {

    /**
     * Tests that when using the ClientOptions you need to configure a
     * container id as that is mandatory and the only reason one would
     * be supplying ClientOptions instances.
     */
    @Test
    public void testCreateWithNoContainerIdFails() {
        ClientOptions options = new ClientOptions();
        assertNull(options.id());

        try {
            Client.create(options);
            fail("Should enforce user supplied container Id");
        } catch (NullPointerException npe) {
            // Expected
        }
    }

    @Test
    public void testCreateWithContainerId() {
        final String id = "test-id";

        ClientOptions options = new ClientOptions();
        options.id(id);
        assertNotNull(options.id());

        Client client = Client.create(options);
        assertNotNull(client.containerId());
        assertEquals(id, client.containerId());
    }

    @Test
    public void testCoseClientAndConnectShouldFail() throws ClientException {
        Client client = Client.create();
        assertTrue(client.close().isDone());

        try {
            client.connect("localhost");
            fail("Should enforce no new connections on Client close");
        } catch (ClientClosedException closed) {
            // Expected
        }

        try {
            client.connect("localhost", new ConnectionOptions());
            fail("Should enforce no new connections on Client close");
        } catch (ClientClosedException closed) {
            // Expected
        }

        try {
            client.connect("localhost", 5672);
            fail("Should enforce no new connections on Client close");
        } catch (ClientClosedException closed) {
            // Expected
        }

        try {
            client.connect("localhost", 5672, new ConnectionOptions());
            fail("Should enforce no new connections on Client close");
        } catch (ClientClosedException closed) {
            // Expected
        }
    }
}
