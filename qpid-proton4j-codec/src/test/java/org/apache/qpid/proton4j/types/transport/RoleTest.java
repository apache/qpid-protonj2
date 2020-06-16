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
package org.apache.qpid.proton4j.types.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.qpid.proton4j.types.transport.Role;
import org.junit.Test;

public class RoleTest {

    @Test
    public void testValueOf() {
        assertEquals(Role.SENDER, Role.valueOf((Boolean) null));
        assertEquals(Role.SENDER, Role.valueOf(Boolean.FALSE));
        assertEquals(Role.RECEIVER, Role.valueOf(Boolean.TRUE));
        assertEquals(Role.SENDER, Role.valueOf(false));
        assertEquals(Role.RECEIVER, Role.valueOf(true));
    }

    @Test
    public void testEquality() {
        Role sender = Role.SENDER;
        Role receiver = Role.RECEIVER;

        assertEquals(sender, Role.valueOf(false));
        assertEquals(receiver, Role.valueOf(true));

        assertEquals(sender.getValue(), Boolean.FALSE);
        assertEquals(receiver.getValue(), Boolean.TRUE);
    }

    @Test
    public void testNotEquality() {
        Role sender = Role.SENDER;
        Role receiver = Role.RECEIVER;

        assertNotEquals(sender, Role.valueOf(true));
        assertNotEquals(receiver, Role.valueOf(false));

        assertNotEquals(sender.getValue(), Boolean.TRUE);
        assertNotEquals(receiver.getValue(), Boolean.FALSE);
    }
}
