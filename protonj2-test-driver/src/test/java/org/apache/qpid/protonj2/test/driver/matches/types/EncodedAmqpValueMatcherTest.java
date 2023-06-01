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

package org.apache.qpid.protonj2.test.driver.matches.types;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.Codec;
import org.apache.qpid.protonj2.test.driver.codec.messaging.AmqpValue;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedAmqpValueMatcher;
import org.junit.jupiter.api.Test;

/**
 * Test encoded AMQP value type matcher
 */
public class EncodedAmqpValueMatcherTest {

    private final Codec codec = Codec.Factory.create();

    @Test
    public void testMatchesString() {
        final EncodedAmqpValueMatcher matcher = new EncodedAmqpValueMatcher("test");

        final ByteBuffer passingValue = encodeProtonPerformative(new AmqpValue("test"));
        final ByteBuffer failingValue = encodeProtonPerformative(new AmqpValue("fail"));

        assertTrue(matcher.matches(passingValue));
        assertFalse(matcher.matches(failingValue));
    }

    @Test
    public void testMatchingMaps() {
        final List<String> list1 = new ArrayList<>();
        list1.add("list1");
        final List<String> list2 = new ArrayList<>();
        list1.add("list2");

        final Map<String, Object> passingMap = new HashMap<>();
        passingMap.put("entry1", "entry-pass");
        passingMap.put("entry2", list1);
        final Map<String, Object> failingMap = new HashMap<>();
        failingMap.put("entry1", "entry-fail");
        failingMap.put("entry2", list2);

        final EncodedAmqpValueMatcher matcher = new EncodedAmqpValueMatcher(passingMap);

        final ByteBuffer passingValue = encodeProtonPerformative(new AmqpValue(passingMap));
        final ByteBuffer failingValue = encodeProtonPerformative(new AmqpValue(failingMap));

        assertTrue(matcher.matches(passingValue));
        assertFalse(matcher.matches(failingValue));
    }

    @Test
    public void testMatchingMapsOfLists() {
        final List<String> list1 = new ArrayList<>();
        list1.add("list1");
        final List<String> list2 = new ArrayList<>();
        list1.add("list2");

        final Map<String, Object> passingMap = new HashMap<>();
        passingMap.put("entry2", list1);
        final Map<String, Object> failingMap = new HashMap<>();
        failingMap.put("entry2", list2);

        final EncodedAmqpValueMatcher matcher = new EncodedAmqpValueMatcher(passingMap);

        final ByteBuffer passingValue = encodeProtonPerformative(new AmqpValue(passingMap));
        final ByteBuffer failingValue = encodeProtonPerformative(new AmqpValue(failingMap));

        assertTrue(matcher.matches(passingValue));
        assertFalse(matcher.matches(failingValue));
    }

    private ByteBuffer encodeProtonPerformative(DescribedType performative) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(256);

        if (performative != null) {
            try (DataOutputStream output = new DataOutputStream(baos)) {
                codec.putDescribedType(performative);
                codec.encode(output);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                codec.clear();
            }
        }

        return ByteBuffer.wrap(baos.toByteArray());
    }
}
