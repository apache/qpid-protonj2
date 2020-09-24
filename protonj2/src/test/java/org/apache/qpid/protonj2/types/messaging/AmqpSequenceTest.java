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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;

import org.apache.qpid.protonj2.types.messaging.Section.SectionType;
import org.junit.jupiter.api.Test;

public class AmqpSequenceTest {

    @Test
    public void testToStringOnEmptyObject() {
        assertNotNull(new AmqpSequence<>(null).toString());
    }

    @Test
    public void testGetSequenceFromEmptySection() {
        assertNull(new AmqpSequence<>(null).getValue());
    }

    @Test
    public void testCopyFromEmpty() {
        assertNull(new AmqpSequence<>(null).copy().getValue());
    }

    @Test
    public void testCopy() {
        ArrayList<Object> payload = new ArrayList<>();
        payload.add("test");

        AmqpSequence<Object> original = new AmqpSequence<>(payload);
        AmqpSequence<Object> copy = original.copy();

        assertNotSame(original, copy);
        assertNotSame(original.getValue(), copy.getValue());
        assertEquals(original.getValue(), copy.getValue());
    }

    @Test
    public void testGetType() {
        assertEquals(SectionType.AmqpSequence, new AmqpSequence<>(null).getType());
    }
}
