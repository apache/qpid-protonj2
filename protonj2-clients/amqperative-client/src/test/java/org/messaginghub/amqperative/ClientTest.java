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
package org.messaginghub.amqperative;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.messaginghub.amqperative.test.AMQPerativeTestCase;

/**
 * Test the Client API implementation
 */
public class ClientTest extends AMQPerativeTestCase {

    @Test
    public void testCreateWithNoContainerId() {
        ClientOptions options = new ClientOptions();
        assertNull(options.getContainerId());

        Client client = Client.create(options);
        assertNotNull(client.getContainerId());
    }

    @Test
    public void testCreateWithContainerId() {
        final String id = "test-id";

        ClientOptions options = new ClientOptions();
        options.setContainerId(id);
        assertNotNull(options.getContainerId());

        Client client = Client.create(options);
        assertNotNull(client.getContainerId());
        assertEquals(id, client.getContainerId());
    }
}
