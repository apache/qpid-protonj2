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
package org.apache.qpid.protonj2.types.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Properties;
import org.apache.qpid.protonj2.types.messaging.Section.SectionType;
import org.junit.Test;

public class PropertiesTypeTest {

    private static final String TEST_MESSAGE_ID = "test";
    private static final Binary TEST_USER_ID = new Binary(new byte[] {1});
    private static final String TEST_TO_ADDRESS = "to";
    private static final String TEST_TO_SUBJECT = "subject";
    private static final String TEST_REPLYTO_ADDRESS = "reply-to";
    private static final String TEST_CORRELATION_ID = "correlation";
    private static final String TEST_CONTENT_TYPE = "text/test";
    private static final String TEST_CONTENT_ENCODING = "UTF-8";
    private static final long TEST_ABSOLUTE_EXPIRY_TIME = 100l;
    private static final long TEST_CREATION_TIME = 200l;
    private static final String TEST_GROUP_ID = "group-test";
    private static final long TEST_GROUP_SEQUENCE = 300l;
    private static final String TEST_REPLYTO_GROUPID = "reply-to-group";

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Properties().toString());
    }

    @Test
    public void testGetType() {
        assertEquals(SectionType.Properties, new Properties().getType());
    }

    @Test
    public void testCreate() {
        Properties properties = new Properties();

        assertNull(properties.getMessageId());
        assertNull(properties.getUserId());
        assertNull(properties.getTo());
        assertNull(properties.getSubject());
        assertNull(properties.getReplyTo());
        assertNull(properties.getCorrelationId());
        assertNull(properties.getContentType());
        assertNull(properties.getContentEncoding());
        assertEquals(0, properties.getAbsoluteExpiryTime());
        assertEquals(0, properties.getCreationTime());
        assertNull(properties.getGroupId());
        assertEquals(0, properties.getGroupSequence());
        assertNull(properties.getReplyToGroupId());

        assertTrue(properties.isEmpty());
        assertEquals(0, properties.getElementCount());
    }

    @Test
    public void testCopyFromDefault() {
        Properties properties = new Properties();

        assertNull(properties.getMessageId());
        assertNull(properties.getUserId());
        assertNull(properties.getTo());
        assertNull(properties.getSubject());
        assertNull(properties.getReplyTo());
        assertNull(properties.getCorrelationId());
        assertNull(properties.getContentType());
        assertNull(properties.getContentEncoding());
        assertEquals(0, properties.getAbsoluteExpiryTime());
        assertEquals(0, properties.getCreationTime());
        assertNull(properties.getGroupId());
        assertEquals(0, properties.getGroupSequence());
        assertNull(properties.getReplyToGroupId());
        assertTrue(properties.isEmpty());
        assertEquals(0, properties.getElementCount());

        Properties copy = properties.copy();

        assertNull(copy.getMessageId());
        assertNull(copy.getUserId());
        assertNull(copy.getTo());
        assertNull(copy.getSubject());
        assertNull(copy.getReplyTo());
        assertNull(copy.getCorrelationId());
        assertNull(copy.getContentType());
        assertNull(copy.getContentEncoding());
        assertEquals(0, copy.getAbsoluteExpiryTime());
        assertEquals(0, copy.getCreationTime());
        assertNull(copy.getGroupId());
        assertEquals(0, copy.getGroupSequence());
        assertNull(copy.getReplyToGroupId());
        assertTrue(copy.isEmpty());
        assertEquals(0, copy.getElementCount());
    }

    @Test
    public void testCopyConstructor() {
        Properties properties = new Properties();

        properties.setMessageId(TEST_MESSAGE_ID);
        properties.setUserId(TEST_USER_ID);
        properties.setTo(TEST_TO_ADDRESS);
        properties.setSubject(TEST_TO_SUBJECT);
        properties.setReplyTo(TEST_REPLYTO_ADDRESS);
        properties.setCorrelationId(TEST_CORRELATION_ID);
        properties.setContentType(TEST_CONTENT_TYPE);
        properties.setContentEncoding(TEST_CONTENT_ENCODING);
        properties.setAbsoluteExpiryTime(TEST_ABSOLUTE_EXPIRY_TIME);
        properties.setCreationTime(TEST_CREATION_TIME);
        properties.setGroupId(TEST_GROUP_ID);
        properties.setGroupSequence(TEST_GROUP_SEQUENCE);
        properties.setReplyToGroupId(TEST_REPLYTO_GROUPID);

        Properties copy = new Properties(properties);

        assertFalse(copy.isEmpty());

        assertTrue(copy.hasMessageId());
        assertTrue(copy.hasUserId());
        assertTrue(copy.hasTo());
        assertTrue(copy.hasSubject());
        assertTrue(copy.hasReplyTo());
        assertTrue(copy.hasCorrelationId());
        assertTrue(copy.hasContentType());
        assertTrue(copy.hasContentEncoding());
        assertTrue(copy.hasAbsoluteExpiryTime());
        assertTrue(copy.hasCreationTime());
        assertTrue(copy.hasGroupId());
        assertTrue(copy.hasGroupSequence());
        assertTrue(copy.hasReplyToGroupId());

        // Check boolean has methods
        assertEquals(properties.hasMessageId(), copy.hasMessageId());
        assertEquals(properties.hasUserId(), copy.hasUserId());
        assertEquals(properties.hasTo(), copy.hasTo());
        assertEquals(properties.hasSubject(), copy.hasSubject());
        assertEquals(properties.hasReplyTo(), copy.hasReplyTo());
        assertEquals(properties.hasCorrelationId(), copy.hasCorrelationId());
        assertEquals(properties.hasContentType(), copy.hasContentType());
        assertEquals(properties.hasContentEncoding(), copy.hasContentEncoding());
        assertEquals(properties.hasAbsoluteExpiryTime(), copy.hasAbsoluteExpiryTime());
        assertEquals(properties.hasCreationTime(), copy.hasCreationTime());
        assertEquals(properties.hasGroupId(), copy.hasGroupId());
        assertEquals(properties.hasGroupSequence(), copy.hasGroupSequence());
        assertEquals(properties.hasReplyToGroupId(), copy.hasReplyToGroupId());

        // Test actual values copied
        assertEquals(properties.getMessageId(), copy.getMessageId());
        assertEquals(properties.getUserId(), copy.getUserId());
        assertEquals(properties.getTo(), copy.getTo());
        assertEquals(properties.getSubject(), copy.getSubject());
        assertEquals(properties.getReplyTo(), copy.getReplyTo());
        assertEquals(properties.getCorrelationId(), copy.getCorrelationId());
        assertEquals(properties.getContentType(), copy.getContentType());
        assertEquals(properties.getContentEncoding(), copy.getContentEncoding());
        assertEquals(properties.getAbsoluteExpiryTime(), copy.getAbsoluteExpiryTime());
        assertEquals(properties.getCreationTime(), copy.getCreationTime());
        assertEquals(properties.getGroupId(), copy.getGroupId());
        assertEquals(properties.getGroupSequence(), copy.getGroupSequence());
        assertEquals(properties.getReplyToGroupId(), copy.getReplyToGroupId());

        assertEquals(properties.getElementCount(), copy.getElementCount());
    }

    @Test
    public void testGetElementCount() {
        Properties properties = new Properties();

        assertTrue(properties.isEmpty());
        assertEquals(0, properties.getElementCount());

        properties.setMessageId("ID");

        assertFalse(properties.isEmpty());
        assertEquals(1, properties.getElementCount());

        properties.setMessageId(null);

        assertTrue(properties.isEmpty());
        assertEquals(0, properties.getElementCount());

        properties.setReplyToGroupId("ID");
        assertFalse(properties.isEmpty());
        assertEquals(13, properties.getElementCount());

        properties.setMessageId("ID");
        assertFalse(properties.isEmpty());
        assertEquals(13, properties.getElementCount());
    }

    @Test
    public void testMessageId() {
        Properties properties = new Properties();

        assertFalse(properties.hasMessageId());
        assertNull(properties.getMessageId());

        properties.setMessageId("ID");
        assertTrue(properties.hasMessageId());
        assertNotNull(properties.getMessageId());

        properties.setMessageId(null);
        assertFalse(properties.hasMessageId());
        assertNull(properties.getMessageId());
    }

    @Test
    public void testUserId() {
        Properties properties = new Properties();

        assertFalse(properties.hasUserId());
        assertNull(properties.getUserId());

        properties.setUserId(new Binary("ID".getBytes(StandardCharsets.UTF_8)));
        assertTrue(properties.hasUserId());
        assertNotNull(properties.getUserId());

        properties.setUserId(null);
        assertFalse(properties.hasUserId());
        assertNull(properties.getUserId());
    }

    @Test
    public void testTo() {
        Properties properties = new Properties();

        assertFalse(properties.hasTo());
        assertNull(properties.getTo());

        properties.setTo("ID");
        assertTrue(properties.hasTo());
        assertNotNull(properties.getTo());

        properties.setTo(null);
        assertFalse(properties.hasTo());
        assertNull(properties.getTo());
    }

    @Test
    public void testSubject() {
        Properties properties = new Properties();

        assertFalse(properties.hasSubject());
        assertNull(properties.getSubject());

        properties.setSubject("ID");
        assertTrue(properties.hasSubject());
        assertNotNull(properties.getSubject());

        properties.setSubject(null);
        assertFalse(properties.hasSubject());
        assertNull(properties.getSubject());
    }

    @Test
    public void testReplyTo() {
        Properties properties = new Properties();

        assertFalse(properties.hasReplyTo());
        assertNull(properties.getReplyTo());

        properties.setReplyTo("ID");
        assertTrue(properties.hasReplyTo());
        assertNotNull(properties.getReplyTo());

        properties.setReplyTo(null);
        assertFalse(properties.hasReplyTo());
        assertNull(properties.getReplyTo());
    }

    @Test
    public void testCorrelationId() {
        Properties properties = new Properties();

        assertFalse(properties.hasCorrelationId());
        assertNull(properties.getCorrelationId());

        properties.setCorrelationId("ID");
        assertTrue(properties.hasCorrelationId());
        assertNotNull(properties.getCorrelationId());

        properties.setCorrelationId(null);
        assertFalse(properties.hasCorrelationId());
        assertNull(properties.getCorrelationId());
    }

    @Test
    public void testContentType() {
        Properties properties = new Properties();

        assertFalse(properties.hasContentType());
        assertNull(properties.getContentType());

        properties.setContentType("ID");
        assertTrue(properties.hasContentType());
        assertNotNull(properties.getContentType());

        properties.setContentType(null);
        assertFalse(properties.hasContentType());
        assertNull(properties.getContentType());
    }

    @Test
    public void testContentEncoding() {
        Properties properties = new Properties();

        assertFalse(properties.hasContentEncoding());
        assertNull(properties.getContentEncoding());

        properties.setContentEncoding("ID");
        assertTrue(properties.hasContentEncoding());
        assertNotNull(properties.getContentEncoding());

        properties.setContentEncoding(null);
        assertFalse(properties.hasContentEncoding());
        assertNull(properties.getContentEncoding());
    }

    @Test
    public void testAbsoluteExpiryTime() {
        Properties properties = new Properties();

        assertFalse(properties.hasAbsoluteExpiryTime());
        assertEquals(0, properties.getAbsoluteExpiryTime());

        properties.setAbsoluteExpiryTime(2048);
        assertTrue(properties.hasAbsoluteExpiryTime());
        assertEquals(2048, properties.getAbsoluteExpiryTime());

        properties.clearAbsoluteExpiryTime();
        assertFalse(properties.hasAbsoluteExpiryTime());
        assertEquals(0, properties.getAbsoluteExpiryTime());
    }

    @Test
    public void testCreationTime() {
        Properties properties = new Properties();

        assertFalse(properties.hasCreationTime());
        assertEquals(0, properties.getCreationTime());

        properties.setCreationTime(2048);
        assertTrue(properties.hasCreationTime());
        assertEquals(2048, properties.getCreationTime());

        properties.clearCreationTime();
        assertFalse(properties.hasCreationTime());
        assertEquals(0, properties.getCreationTime());
    }

    @Test
    public void testGroupId() {
        Properties properties = new Properties();

        assertFalse(properties.hasGroupId());
        assertNull(properties.getGroupId());

        properties.setGroupId("ID");
        assertTrue(properties.hasGroupId());
        assertNotNull(properties.getGroupId());

        properties.setGroupId(null);
        assertFalse(properties.hasGroupId());
        assertNull(properties.getGroupId());
    }

    @Test
    public void testGroupSequence() {
        Properties properties = new Properties();

        assertFalse(properties.hasGroupSequence());
        assertEquals(0, properties.getGroupSequence());

        properties.setGroupSequence(2048);
        assertTrue(properties.hasGroupSequence());
        assertEquals(2048, properties.getGroupSequence());

        properties.clearGroupSequence();
        assertFalse(properties.hasGroupSequence());
        assertEquals(0, properties.getGroupSequence());

        try {
            properties.setGroupSequence(UnsignedInteger.MAX_VALUE.longValue() + 1);
            fail("Should perform range check on set value");
        } catch (IllegalArgumentException iae) {}

        try {
            properties.setGroupSequence(-1);
            fail("Should perform range check on set value");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testReplyToGroupId() {
        Properties properties = new Properties();

        assertFalse(properties.hasReplyToGroupId());
        assertNull(properties.getReplyToGroupId());

        properties.setReplyToGroupId("ID");
        assertTrue(properties.hasReplyToGroupId());
        assertNotNull(properties.getReplyToGroupId());

        properties.setReplyToGroupId(null);
        assertFalse(properties.hasReplyToGroupId());
        assertNull(properties.getReplyToGroupId());
    }
}

