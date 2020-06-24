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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.transport.AmqpError;
import org.apache.qpid.protonj2.types.transport.ErrorCondition;
import org.junit.Test;

public class ErrorConditionTest {

    @Test
    public void testToStringOnFreshInstance() {
        assertNotNull(new ErrorCondition(AmqpError.DECODE_ERROR, (String) null).toString());
    }

    @SuppressWarnings("unlikely-arg-type")
    @Test
    public void testEquals() {
        ErrorCondition original = new ErrorCondition(AmqpError.DECODE_ERROR, "error");
        ErrorCondition copy = original.copy();

        assertEquals(original, copy);

        Map<Symbol, Object> infoMap = new HashMap<>();
        ErrorCondition other1 = new ErrorCondition(null, "error", infoMap);
        ErrorCondition other2 = new ErrorCondition(AmqpError.DECODE_ERROR, null, infoMap);
        ErrorCondition other3 = new ErrorCondition(AmqpError.DECODE_ERROR, "error", infoMap);
        ErrorCondition other4 = new ErrorCondition(null, null, infoMap);
        ErrorCondition other5 = new ErrorCondition(null, null, null);

        assertNotEquals(original, other1);
        assertNotEquals(original, other2);
        assertNotEquals(original, other3);
        assertNotEquals(original, other4);
        assertNotEquals(original, other5);

        assertNotEquals(other1, original);
        assertNotEquals(other2, original);
        assertNotEquals(other3, original);
        assertNotEquals(other4, original);
        assertNotEquals(other5, original);

        assertFalse(original.equals(null));
        assertFalse(original.equals(Boolean.TRUE));
    }

    @Test
    public void testCopyFromNew() {
        ErrorCondition original = new ErrorCondition(AmqpError.DECODE_ERROR, "error");
        ErrorCondition copy = original.copy();

        assertEquals(original.getCondition(), copy.getCondition());
        assertEquals(original.getDescription(), copy.getDescription());
        assertEquals(original.getInfo(), copy.getInfo());
    }
}
