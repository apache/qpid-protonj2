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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.qpid.protonj2.types.messaging.Section.SectionType;
import org.junit.jupiter.api.Test;

public class AmqpValueTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new AmqpValue<>(null).toString());
    }

    @Test
    public void testGetValueFromEmptySection() {
        assertNull(new AmqpValue<>(null).getValue());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new AmqpValue<>(null).copy().getValue());
    }

    @Test
    public void testGetType() {
        assertEquals(SectionType.AmqpValue, new AmqpValue<>(null).getType());
    }

    @Test
    public void testHashCode() {
        String first = new String("first");
        String second = new String("second");

        AmqpValue<String> original = new AmqpValue<>(first);
        AmqpValue<String> copy = original.copy();
        AmqpValue<String> another = new AmqpValue<>(second);

        assertEquals(original.hashCode(), copy.hashCode());
        assertNotEquals(original.hashCode(), another.hashCode());

        AmqpValue<String> empty = new AmqpValue<>(null);
        AmqpValue<String> empty2 = new AmqpValue<>(null);

        assertEquals(empty2.hashCode(), empty.hashCode());
        assertNotEquals(original.hashCode(), empty.hashCode());
    }

    @Test
    public void testEquals() {
        String first = new String("first");
        String second = new String("second");

        AmqpValue<String> original = new AmqpValue<>(first);
        AmqpValue<String> copy = original.copy();
        AmqpValue<String> another = new AmqpValue<>(second);
        AmqpValue<String> empty = new AmqpValue<>(null);
        AmqpValue<String> empty2 = new AmqpValue<>(null);

        assertEquals(original, original);
        assertEquals(original, copy);
        assertNotEquals(original, another);
        assertNotEquals(original, "test");
        assertNotEquals(original, empty);
        assertNotEquals(empty, original);
        assertEquals(empty, empty2);

        assertFalse(original.equals(null));
    }
}
