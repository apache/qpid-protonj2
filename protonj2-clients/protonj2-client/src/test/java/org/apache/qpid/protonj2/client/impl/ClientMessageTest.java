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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.protonj2.client.AdvancedMessage;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.types.messaging.AmqpSequence;
import org.apache.qpid.protonj2.types.messaging.AmqpValue;
import org.apache.qpid.protonj2.types.messaging.ApplicationProperties;
import org.apache.qpid.protonj2.types.messaging.Data;
import org.apache.qpid.protonj2.types.messaging.Footer;
import org.apache.qpid.protonj2.types.messaging.Header;
import org.apache.qpid.protonj2.types.messaging.MessageAnnotations;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section;
import org.junit.jupiter.api.Test;

/**
 * Test the API of {@link ClientMessage}
 */
class ClientMessageTest {

    @Test
    public void testCreateEmpty() {
        ClientMessage<String> message = ClientMessage.create();

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());

        assertFalse(message.hasApplicationProperties());
        assertFalse(message.hasFooters());
        assertFalse(message.hasMessageAnnotations());
    }

    @Test
    public void testCreateEmptyAdvanced() {
        AdvancedMessage<String> message = ClientMessage.createAdvancedMessage();

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());

        assertFalse(message.hasApplicationProperties());
        assertFalse(message.hasFooters());
        assertFalse(message.hasMessageAnnotations());

        assertNull(message.header());
        assertNull(message.annotations());
        assertNull(message.applicationProperties());
        assertNull(message.footer());

        Header header = new Header();
        Properties properties = new Properties();
        MessageAnnotations ma = new MessageAnnotations(new LinkedHashMap<>());
        ApplicationProperties ap = new ApplicationProperties(new LinkedHashMap<>());
        Footer ft = new Footer(new LinkedHashMap<>());

        message.header(header);
        message.properties(properties);
        message.annotations(ma);
        message.applicationProperties(ap);
        message.footer(ft);

        assertSame(header, message.header());
        assertSame(properties, message.properties());
        assertSame(ma, message.annotations());
        assertSame(ap, message.applicationProperties());
        assertSame(ft, message.footer());
    }

    @Test
    public void testCreateWithBody() {
        ClientMessage<String> message = ClientMessage.create(new AmqpValue<String>("test"));

        assertNotNull(message.body());
        assertNotNull(message.bodySections());
        assertFalse(message.bodySections().isEmpty());

        assertEquals("test", message.body());

        message.forEachBodySection(value -> {
            assertEquals(new AmqpValue<>("test"), value);
        });
    }

    @Test
    public void testToAdvancedMessageReturnsSameInstance() {
        Message<String> message = ClientMessage.create(new AmqpValue<String>("test"));

        assertNotNull(message.body());

        AdvancedMessage<String> advanced = message.toAdvancedMessage();

        assertSame(message, advanced);

        assertNotNull(advanced.bodySections());
        assertFalse(advanced.bodySections().isEmpty());

        assertEquals("test", advanced.body());

        advanced.forEachBodySection(value -> {
            assertEquals(new AmqpValue<>("test"), value);
        });

        assertEquals(0, advanced.messageFormat());
        advanced.messageFormat(17);
        assertEquals(17, advanced.messageFormat());
    }

    @Test
    public void testSetGetHeaderFields() {
        ClientMessage<String> message = ClientMessage.create();

        assertEquals(Header.DEFAULT_DURABILITY, message.durable());
        assertEquals(Header.DEFAULT_FIRST_ACQUIRER, message.firstAcquirer());
        assertEquals(Header.DEFAULT_DELIVERY_COUNT, message.deliveryCount());
        assertEquals(Header.DEFAULT_TIME_TO_LIVE, message.timeToLive());
        assertEquals(Header.DEFAULT_PRIORITY, message.priority());

        message.durable(true);
        message.firstAcquirer(true);
        message.deliveryCount(10);
        message.timeToLive(11);
        message.priority((byte) 12);

        assertEquals(true, message.durable());
        assertEquals(true, message.firstAcquirer());
        assertEquals(10, message.deliveryCount());
        assertEquals(11, message.timeToLive());
        assertEquals(12, message.priority());
    }

    @Test
    public void testSetGetMessagePropertiesFields() {
        ClientMessage<String> message = ClientMessage.create();

        assertNull(message.messageId());
        assertNull(message.userId());
        assertNull(message.to());
        assertNull(message.subject());
        assertNull(message.replyTo());
        assertNull(message.correlationId());
        assertNull(message.contentType());
        assertNull(message.contentEncoding());
        assertEquals(0, message.creationTime());
        assertEquals(0, message.absoluteExpiryTime());
        assertNull(message.groupId());
        assertEquals(0, message.groupSequence());
        assertNull(message.replyToGroupId());

        message.messageId("message-id");
        message.userId("user-id".getBytes(StandardCharsets.UTF_8));
        message.to("to");
        message.subject("subject");
        message.replyTo("replyTo");
        message.correlationId("correlationId");
        message.contentType("contentType");
        message.contentEncoding("contentEncoding");
        message.creationTime(32);
        message.absoluteExpiryTime(64);
        message.groupId("groupId");
        message.groupSequence(128);
        message.replyToGroupId("replyToGroupId");

        assertEquals("message-id", message.messageId());
        assertEquals("user-id", new String(message.userId(), StandardCharsets.UTF_8));
        assertEquals("to", message.to());
        assertEquals("subject", message.subject());
        assertEquals("replyTo", message.replyTo());
        assertEquals("subject", message.subject());
        assertEquals("correlationId", message.correlationId());
        assertEquals("contentType", message.contentType());
        assertEquals("contentEncoding", message.contentEncoding());
        assertEquals(32, message.creationTime());
        assertEquals(64, message.absoluteExpiryTime());
        assertEquals("groupId", message.groupId());
        assertEquals(128, message.groupSequence());
        assertEquals("replyToGroupId", message.replyToGroupId());
    }

    @Test
    public void testBodySetGet() {
        ClientMessage<String> message = ClientMessage.create();

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());

        assertNotNull(message.body("test"));
        assertEquals("test", message.body());

        message.forEachBodySection(value -> {
            assertEquals(new AmqpValue<>("test"), value);
        });

        message.clearBodySections();

        assertEquals(0, message.bodySections().size());
        assertNull(message.body());

        final AtomicInteger count = new AtomicInteger();
        message.bodySections().forEach(value -> {
            count.incrementAndGet();
        });

        assertEquals(0, count.get());
    }

    @Test
    public void testForEachMethodsOnEmptyMessage() {
        ClientMessage<String> message = ClientMessage.create();

        assertFalse(message.hasApplicationProperties());
        assertFalse(message.hasFooters());
        assertFalse(message.hasMessageAnnotations());

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());

        message.forEachBodySection(value -> {
            fail("Should not invoke any consumers since Message is empty");
        });

        message.forEachApplicationProperty((key, value) -> {
            fail("Should not invoke any consumers since Message is empty");
        });

        message.forEachFooter((key, value) -> {
            fail("Should not invoke any consumers since Message is empty");
        });

        message.forEachMessageAnnotation((key, value) -> {
            fail("Should not invoke any consumers since Message is empty");
        });
    }

    @Test
    public void testSetMultipleBodySections() {
        ClientMessage<String> message = ClientMessage.create();

        List<Section<?>> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));
        expected.add(new Data(new byte[] { 1 }));
        expected.add(new Data(new byte[] { 2 }));

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());

        message.bodySections(expected);

        assertEquals(expected.size(), message.bodySections().size());

        final AtomicInteger count = new AtomicInteger();
        message.forEachBodySection(value -> {
            assertEquals(expected.get(count.get()), value);
            count.incrementAndGet();
        });

        assertEquals(expected.size(), count.get());

        count.set(0);
        message.bodySections().forEach(value -> {
            assertEquals(expected.get(count.get()), value);
            count.incrementAndGet();
        });

        assertEquals(expected.size(), count.get());

        message.bodySections(Collections.emptyList());

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());

        message.bodySections(expected);

        assertEquals(expected.size(), message.bodySections().size());

        message.bodySections(null);

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());
    }

    @Test
    public void testAddMultipleBodySections() {
        ClientMessage<byte[]> message = ClientMessage.create();

        List<Data> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));
        expected.add(new Data(new byte[] { 1 }));
        expected.add(new Data(new byte[] { 2 }));

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());

        for (Data value : expected) {
            message.addBodySection(value);
        }

        assertEquals(expected.size(), message.bodySections().size());

        final AtomicInteger count = new AtomicInteger();
        message.forEachBodySection(value -> {
            assertEquals(expected.get(count.get()), value);
            count.incrementAndGet();
        });

        assertEquals(expected.size(), count.get());

        count.set(0);
        message.bodySections().forEach(value -> {
            assertEquals(expected.get(count.get()), value);
            count.incrementAndGet();
        });

        assertEquals(expected.size(), count.get());

        message.clearBodySections();

        assertEquals(0, message.bodySections().size());

        count.set(0);
        message.bodySections().forEach(value -> {
            count.incrementAndGet();
        });

        assertEquals(0, count.get());
    }

    @Test
    public void testMixSingleAndMultipleSectionAccess() {
        ClientMessage<byte[]> message = ClientMessage.create();

        List<Data> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));
        expected.add(new Data(new byte[] { 1 }));
        expected.add(new Data(new byte[] { 2 }));

        assertNull(message.body());
        assertNotNull(message.bodySections());
        assertTrue(message.bodySections().isEmpty());

        message.body(expected.get(0).getValue());

        assertEquals(expected.get(0).getValue(), message.body());
        assertNotNull(message.bodySections());
        assertFalse(message.bodySections().isEmpty());
        assertEquals(1, message.bodySections().size());

        message.addBodySection(expected.get(1));

        assertEquals(expected.get(0).getValue(), message.body());
        assertNotNull(message.bodySections());
        assertFalse(message.bodySections().isEmpty());
        assertEquals(2, message.bodySections().size());

        message.addBodySection(expected.get(2));

        assertEquals(expected.get(0).getValue(), message.body());
        assertNotNull(message.bodySections());
        assertFalse(message.bodySections().isEmpty());
        assertEquals(3, message.bodySections().size());

        final AtomicInteger count = new AtomicInteger();
        message.bodySections().forEach(value -> {
            assertEquals(expected.get(count.get()), value);
            count.incrementAndGet();
        });

        assertEquals(expected.size(), count.get());
    }

    @Test
    public void testSetMultipleBodySectionsValidatesDefaultFormat() {
        ClientMessage<Object> message = ClientMessage.create();

        List<Section<?>> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));
        expected.add(new AmqpValue<String>("test"));
        expected.add(new AmqpSequence<>(new ArrayList<>()));

        assertThrows(IllegalArgumentException.class, () -> message.bodySections(expected));
    }

    @Test
    public void testAddMultipleBodySectionsValidatesDefaultFormat() {
        ClientMessage<Object> message = ClientMessage.create();

        List<Section<?>> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));
        expected.add(new AmqpValue<String>("test"));
        expected.add(new AmqpSequence<>(new ArrayList<>()));

        assertThrows(IllegalArgumentException.class, () -> expected.forEach(section -> message.addBodySection(section)));
    }

    @Test
    public void testReplaceOriginalWithSetBodySectionDoesNotThrowValidationErrorIfValid() {
        ClientMessage<Object> message = ClientMessage.create();

        message.body("string");  // AmqpValue

        List<Section<?>> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));

        assertDoesNotThrow(() -> message.bodySections(expected));
    }

    @Test
    public void testReplaceOriginalWithSetBodySectionDoesThrowValidationErrorIfInValid() {
        ClientMessage<Object> message = ClientMessage.create();

        message.body("string");  // AmqpValue

        List<Section<?>> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));
        expected.add(new AmqpValue<String>("test"));
        expected.add(new AmqpSequence<>(new ArrayList<>()));

        assertThrows(IllegalArgumentException.class, () -> message.bodySections(expected));
    }

    @Test
    public void testAddAdditionalBodySectionsValidatesDefaultFormat() {
        ClientMessage<Object> message = ClientMessage.create();

        message.body("string");  // AmqpValue

        assertThrows(IllegalArgumentException.class, () -> message.addBodySection(new Data(new byte[] { 0 })));
    }

    @Test
    public void testSetMultipleBodySectionsWithNonDefaultMessageFormat() {
        ClientMessage<Object> message = ClientMessage.create().messageFormat(1);

        List<Section<?>> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));
        expected.add(new AmqpValue<String>("test"));
        expected.add(new AmqpSequence<>(new ArrayList<>()));

        assertDoesNotThrow(() -> message.bodySections(expected));

        final AtomicInteger count = new AtomicInteger();
        message.bodySections().forEach(value -> {
            assertEquals(expected.get(count.get()), value);
            count.incrementAndGet();
        });

        assertEquals(expected.size(), count.get());
    }

    @Test
    public void testAddMultipleBodySectionsWithNonDefaultMessageFormat() {
        ClientMessage<Object> message = ClientMessage.create().messageFormat(1);

        List<Section<?>> expected = new ArrayList<>();
        expected.add(new Data(new byte[] { 0 }));
        expected.add(new AmqpValue<String>("test"));
        expected.add(new AmqpSequence<>(new ArrayList<>()));

        assertDoesNotThrow(() -> message.bodySections(expected));

        final AtomicInteger count = new AtomicInteger();
        message.bodySections().forEach(value -> {
            assertEquals(expected.get(count.get()), value);
            count.incrementAndGet();
        });

        assertEquals(expected.size(), count.get());
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

        assertEquals("1", message.removeMessageAnnotation("test1"));
        assertEquals("2", message.removeMessageAnnotation("test2"));
        assertNull(message.removeMessageAnnotation("test1"));
        assertNull(message.removeMessageAnnotation("test2"));
        assertNull(message.removeMessageAnnotation("test3"));
        assertFalse(message.hasMessageAnnotations());
        assertFalse(message.hasMessageAnnotation("test1"));
        assertFalse(message.hasMessageAnnotation("test2"));

        message.forEachMessageAnnotation((k, v) -> {
            fail("Should not be any remaining Message Annotations");
        });
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

        assertEquals("1", message.removeApplicationProperty("test1"));
        assertEquals("2", message.removeApplicationProperty("test2"));
        assertNull(message.removeApplicationProperty("test1"));
        assertNull(message.removeApplicationProperty("test2"));
        assertNull(message.removeApplicationProperty("test3"));
        assertFalse(message.hasApplicationProperties());
        assertFalse(message.hasApplicationProperty("test1"));
        assertFalse(message.hasApplicationProperty("test2"));

        message.forEachApplicationProperty((k, v) -> {
            fail("Should not be any remaining Application Properties");
        });
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

        assertEquals("1", message.removeFooter("test1"));
        assertEquals("2", message.removeFooter("test2"));
        assertNull(message.removeFooter("test1"));
        assertNull(message.removeFooter("test2"));
        assertNull(message.removeFooter("test3"));
        assertFalse(message.hasFooters());
        assertFalse(message.hasFooter("test1"));
        assertFalse(message.hasFooter("test2"));

        message.forEachFooter((k, v) -> {
            fail("Should not be any remaining footers");
        });
    }
}
