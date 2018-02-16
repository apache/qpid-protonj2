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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class HeaderTest {

    @Test
    public void testCreate() {
        Header header = new Header();

        assertFalse(header.hasDurable());
        assertFalse(header.hasPriority());
        assertFalse(header.hasTimeToLive());
        assertFalse(header.hasFirstAcquirer());
        assertFalse(header.hasDeliveryCount());

        assertEquals(Header.DEFAULT_DURABILITY, header.isDurable());
        assertEquals(Header.DEFAULT_PRIORITY, header.getPriority());
        assertEquals(Header.DEFAULT_TIME_TO_LIVE, header.getTimeToLive());
        assertEquals(Header.DEFAULT_FIRST_ACQUIRER, header.isFirstAcquirer());
        assertEquals(Header.DEFAULT_DELIVERY_COUNT, header.getDeliveryCount());
    }

    @Test
    public void testCopy() {
        Header header = new Header();

        header.setDurable(!Header.DEFAULT_DURABILITY);
        header.setPriority((byte) (Header.DEFAULT_PRIORITY + 1));
        header.setTimeToLive(Header.DEFAULT_TIME_TO_LIVE + 10);
        header.setFirstAcquirer(!Header.DEFAULT_FIRST_ACQUIRER);
        header.setDeliveryCount(Header.DEFAULT_DELIVERY_COUNT + 5);

        Header copy = new Header(header);

        assertEquals(!Header.DEFAULT_DURABILITY, copy.isDurable());
        assertEquals(Header.DEFAULT_PRIORITY + 1, copy.getPriority());
        assertEquals(Header.DEFAULT_TIME_TO_LIVE + 10, copy.getTimeToLive());
        assertEquals(!Header.DEFAULT_FIRST_ACQUIRER, copy.isFirstAcquirer());
        assertEquals(Header.DEFAULT_DELIVERY_COUNT + 5, copy.getDeliveryCount());
    }

    @Test
    public void testClearDurable() {
        Header header = new Header();

        assertFalse(header.hasDurable());
        assertEquals(Header.DEFAULT_DURABILITY, header.isDurable());

        header.setDurable(!Header.DEFAULT_DURABILITY);
        assertTrue(header.hasDurable());
        assertNotEquals(Header.DEFAULT_DURABILITY, header.isDurable());

        header.clearDurable();
        assertFalse(header.hasDurable());
        assertEquals(Header.DEFAULT_DURABILITY, header.isDurable());
    }

    @Test
    public void testClearPriority() {
        Header header = new Header();

        assertFalse(header.hasPriority());
        assertEquals(Header.DEFAULT_PRIORITY, header.getPriority());

        header.setPriority((byte) (Header.DEFAULT_PRIORITY + 1));
        assertTrue(header.hasPriority());
        assertNotEquals(Header.DEFAULT_PRIORITY, header.getPriority());

        header.clearPriority();
        assertFalse(header.hasPriority());
        assertEquals(Header.DEFAULT_PRIORITY, header.getPriority());
    }

    @Test
    public void testClearTimeToLive() {
        Header header = new Header();

        assertFalse(header.hasTimeToLive());
        assertEquals(Header.DEFAULT_TIME_TO_LIVE, header.getTimeToLive());

        header.setTimeToLive(Header.DEFAULT_TIME_TO_LIVE + 10);
        assertTrue(header.hasTimeToLive());
        assertNotEquals(Header.DEFAULT_TIME_TO_LIVE, header.getTimeToLive());

        header.clearTimeToLive();
        assertFalse(header.hasTimeToLive());
        assertEquals(Header.DEFAULT_TIME_TO_LIVE, header.getTimeToLive());
    }

    @Test
    public void testClearFirstAcquirer() {
        Header header = new Header();

        assertFalse(header.hasFirstAcquirer());
        assertEquals(Header.DEFAULT_FIRST_ACQUIRER, header.isFirstAcquirer());

        header.setFirstAcquirer(!Header.DEFAULT_FIRST_ACQUIRER);
        assertTrue(header.hasFirstAcquirer());
        assertNotEquals(Header.DEFAULT_FIRST_ACQUIRER, header.isFirstAcquirer());

        header.clearFirstAcquirer();
        assertFalse(header.hasFirstAcquirer());
        assertEquals(Header.DEFAULT_FIRST_ACQUIRER, header.isFirstAcquirer());
    }

    @Test
    public void testClearDeliveryCount() {
        Header header = new Header();

        assertFalse(header.hasDeliveryCount());
        assertEquals(Header.DEFAULT_DELIVERY_COUNT, header.getDeliveryCount());

        header.setDeliveryCount(Header.DEFAULT_DELIVERY_COUNT + 10);
        assertTrue(header.hasDeliveryCount());
        assertNotEquals(Header.DEFAULT_DELIVERY_COUNT, header.getDeliveryCount());

        header.clearDeliveryCount();
        assertFalse(header.hasDeliveryCount());
        assertEquals(Header.DEFAULT_DELIVERY_COUNT, header.getDeliveryCount());
    }

}
