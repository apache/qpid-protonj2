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
package org.apache.qpid.proton4j.amqp.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.qpid.proton4j.types.UnsignedByte;
import org.apache.qpid.proton4j.types.transport.ReceiverSettleMode;
import org.junit.Test;

public class ReceiverSettleModeTest {

    @Test
    public void testValueOf() {
        assertEquals(ReceiverSettleMode.FIRST, ReceiverSettleMode.valueOf((UnsignedByte) null));
        assertEquals(ReceiverSettleMode.FIRST, ReceiverSettleMode.valueOf(UnsignedByte.valueOf((byte) 0)));
        assertEquals(ReceiverSettleMode.SECOND, ReceiverSettleMode.valueOf(UnsignedByte.valueOf((byte) 1)));
    }

    @Test
    public void testEquality() {
        ReceiverSettleMode first = ReceiverSettleMode.FIRST;
        ReceiverSettleMode second = ReceiverSettleMode.SECOND;

        assertEquals(first, ReceiverSettleMode.valueOf(UnsignedByte.valueOf((byte) 0)));
        assertEquals(second, ReceiverSettleMode.valueOf(UnsignedByte.valueOf((byte) 1)));

        assertEquals(first.getValue(), UnsignedByte.valueOf((byte) 0));
        assertEquals(second.getValue(), UnsignedByte.valueOf((byte) 1));
    }

    @Test
    public void testNotEquality() {
        ReceiverSettleMode first = ReceiverSettleMode.FIRST;
        ReceiverSettleMode second = ReceiverSettleMode.SECOND;

        assertNotEquals(first, ReceiverSettleMode.valueOf(UnsignedByte.valueOf((byte) 1)));
        assertNotEquals(second, ReceiverSettleMode.valueOf(UnsignedByte.valueOf((byte) 0)));

        assertNotEquals(first.getValue(), UnsignedByte.valueOf((byte) 1));
        assertNotEquals(second.getValue(), UnsignedByte.valueOf((byte) 0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgument() {
        ReceiverSettleMode.valueOf(UnsignedByte.valueOf((byte) 2));
    }
}
