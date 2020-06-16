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
package org.apache.qpid.proton4j.types.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.messaging.Received;
import org.apache.qpid.proton4j.types.transport.DeliveryState.DeliveryStateType;
import org.junit.Test;

public class ReceivedTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new Received().toString());
    }

    @Test
    public void testGetType() {
        assertEquals(DeliveryStateType.Received, new Received().getType());
    }

    @Test
    public void testSectionNumber() {
        Received received = new Received();

        assertNull(received.getSectionNumber());
        received.setSectionNumber(UnsignedInteger.valueOf(20));
        assertNotNull(received.getSectionNumber());
    }

    @Test
    public void testSectionOffset() {
        Received received = new Received();

        assertNull(received.getSectionOffset());
        received.setSectionOffset(UnsignedLong.valueOf(20));
        assertNotNull(received.getSectionOffset());
    }
}
