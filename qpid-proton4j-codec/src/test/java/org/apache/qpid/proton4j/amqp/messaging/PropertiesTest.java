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
package org.apache.qpid.proton4j.amqp.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton4j.amqp.Binary;
import org.junit.Test;

public class PropertiesTest {

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

