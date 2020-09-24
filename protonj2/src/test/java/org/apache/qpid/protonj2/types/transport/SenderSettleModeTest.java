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
package org.apache.qpid.protonj2.types.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.qpid.protonj2.types.UnsignedByte;
import org.junit.jupiter.api.Test;

public class SenderSettleModeTest {

    @Test
    public void testValueOf() {
        assertEquals(SenderSettleMode.MIXED, SenderSettleMode.valueOf((UnsignedByte) null));
        assertEquals(SenderSettleMode.UNSETTLED, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 0)));
        assertEquals(SenderSettleMode.SETTLED, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 1)));
        assertEquals(SenderSettleMode.MIXED, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 2)));
    }

    @Test
    public void testEquality() {
        SenderSettleMode unsettled = SenderSettleMode.UNSETTLED;
        SenderSettleMode settled = SenderSettleMode.SETTLED;
        SenderSettleMode mixed = SenderSettleMode.MIXED;

        assertEquals(unsettled, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 0)));
        assertEquals(settled, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 1)));
        assertEquals(mixed, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 2)));

        assertEquals(unsettled.getValue(), UnsignedByte.valueOf((byte) 0));
        assertEquals(settled.getValue(), UnsignedByte.valueOf((byte) 1));
        assertEquals(mixed.getValue(), UnsignedByte.valueOf((byte) 2));
    }

    @Test
    public void testNotEquality() {
        SenderSettleMode unsettled = SenderSettleMode.UNSETTLED;
        SenderSettleMode settled = SenderSettleMode.SETTLED;
        SenderSettleMode mixed = SenderSettleMode.MIXED;

        assertNotEquals(unsettled, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 2)));
        assertNotEquals(settled, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 0)));
        assertNotEquals(mixed, SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 1)));

        assertNotEquals(unsettled.getValue(), UnsignedByte.valueOf((byte) 2));
        assertNotEquals(settled.getValue(), UnsignedByte.valueOf((byte) 0));
        assertNotEquals(mixed.getValue(), UnsignedByte.valueOf((byte) 1));
    }

    @Test
    public void testIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> SenderSettleMode.valueOf(UnsignedByte.valueOf((byte) 3)));
    }
}
