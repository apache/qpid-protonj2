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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.junit.jupiter.api.Test;

/**
 * Test the API of {@link ClientMessage}
 */
class CliemtMessageTest {

    @Test
    public void testCreateEmpty() {
        ClientMessage<String> message = ClientMessage.create();

        assertNull(message.body());
        assertNull(message.getBodySection());
        assertNotNull(message.bodySections());

        assertFalse(message.hasApplicationProperties());
        assertFalse(message.hasDeliveryAnnotations());
        assertFalse(message.hasFooters());
        assertFalse(message.hasMessageAnnotations());
    }

    @Test
    public void testBodySetGet() {
        ClientMessage<String> message = ClientMessage.create();

        assertNull(message.body());
        assertNull(message.getBodySection());
        assertNotNull(message.bodySections());

        assertNotNull(message.body("test"));
        assertEquals("test", message.body());
        assertEquals(new AmqpValue<>("test"), message.getBodySection());

        message.forEachBodySection(value -> {
            assertEquals(new AmqpValue<>("test"), value);
        });
    }

    @Test
    public void testDeliveryAnnotation() {
        ClientMessage<String> message = ClientMessage.create();

        final Map<String, String> expectations = new HashMap<>();
        expectations.put("test1", "1");
        expectations.put("test2", "2");

        assertFalse(message.hasDeliveryAnnotations());
        assertFalse(message.hasDeliveryAnnotation("test1"));

        assertNotNull(message.deliveryAnnotation("test1", "1"));
        assertNotNull(message.deliveryAnnotation("test1"));

        assertTrue(message.hasDeliveryAnnotations());
        assertTrue(message.hasDeliveryAnnotation("test1"));

        assertNotNull(message.deliveryAnnotation("test2", "2"));
        assertNotNull(message.deliveryAnnotation("test2"));

        final AtomicInteger count = new AtomicInteger();

        message.forEachDeliveryAnnotation((k, v) -> {
            assertTrue(expectations.containsKey(k));
            assertEquals(v, expectations.get(k));
            count.incrementAndGet();
        });

        assertEquals(expectations.size(), count.get());
    }

    @Test
    public void testMessageAnnotation() {
        ClientMessage<String> message = ClientMessage.create();

        final Map<String, String> expectations = new HashMap<>();
        expectations.put("test1", "1");
        expectations.put("test2", "2");

        assertFalse(message.hasMessageAnnotations());
        assertFalse(message.hasMessageAnnotation("test1"));

        assertNotNull(message.messageAnnotation("test1", "1"));
        assertNotNull(message.messageAnnotation("test1"));

        assertTrue(message.hasMessageAnnotations());
        assertTrue(message.hasMessageAnnotation("test1"));

        assertNotNull(message.messageAnnotation("test2", "2"));
        assertNotNull(message.messageAnnotation("test2"));

        final AtomicInteger count = new AtomicInteger();

        message.forEachMessageAnnotation((k, v) -> {
            assertTrue(expectations.containsKey(k));
            assertEquals(v, expectations.get(k));
            count.incrementAndGet();
        });

        assertEquals(expectations.size(), count.get());
    }

    @Test
    public void testApplicationProperty() {
        ClientMessage<String> message = ClientMessage.create();

        final Map<String, String> expectations = new HashMap<>();
        expectations.put("test1", "1");
        expectations.put("test2", "2");

        assertFalse(message.hasApplicationProperties());
        assertFalse(message.hasApplicationProperty("test1"));

        assertNotNull(message.applicationProperty("test1", "1"));
        assertNotNull(message.applicationProperty("test1"));

        assertTrue(message.hasApplicationProperties());
        assertTrue(message.hasApplicationProperty("test1"));

        assertNotNull(message.applicationProperty("test2", "2"));
        assertNotNull(message.applicationProperty("test2"));

        final AtomicInteger count = new AtomicInteger();

        message.forEachApplicationProperty((k, v) -> {
            assertTrue(expectations.containsKey(k));
            assertEquals(v, expectations.get(k));
            count.incrementAndGet();
        });

        assertEquals(expectations.size(), count.get());
    }

    @Test
    public void testFooter() {
        ClientMessage<String> message = ClientMessage.create();

        final Map<String, String> expectations = new HashMap<>();
        expectations.put("test1", "1");
        expectations.put("test2", "2");

        assertFalse(message.hasFooters());
        assertFalse(message.hasFooter("test1"));

        assertNotNull(message.footer("test1", "1"));
        assertNotNull(message.footer("test1"));

        assertTrue(message.hasFooters());
        assertTrue(message.hasFooter("test1"));

        assertNotNull(message.footer("test2", "2"));
        assertNotNull(message.footer("test2"));

        final AtomicInteger count = new AtomicInteger();

        message.forEachFooter((k, v) -> {
            assertTrue(expectations.containsKey(k));
            assertEquals(v, expectations.get(k));
            count.incrementAndGet();
        });

        assertEquals(expectations.size(), count.get());
    }
}
