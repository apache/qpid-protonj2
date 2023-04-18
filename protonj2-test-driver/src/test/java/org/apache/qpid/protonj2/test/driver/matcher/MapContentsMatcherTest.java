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

package org.apache.qpid.protonj2.test.driver.matcher;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.matchers.MapContentsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.MapContentsMatcher.MatcherMode;
import org.junit.jupiter.api.Test;

/**
 * Test the custom {@link Map} contents matcher type
 */
public class MapContentsMatcherTest {

    @Test
    public void testEmptyMapsAreEqualInAllModes() {
        final Map<String, String> map = new HashMap<>();

        MapContentsMatcher<String, String> matcher1 = new MapContentsMatcher<>(MatcherMode.PARTIAL_MATCH);
        MapContentsMatcher<String, String> matcher2 = new MapContentsMatcher<>(MatcherMode.CONTENTS_MATCH);
        MapContentsMatcher<String, String> matcher3 = new MapContentsMatcher<>(MatcherMode.EXACT_MATCH);

        assertTrue(matcher1.matches(map));
        assertTrue(matcher2.matches(map));
        assertTrue(matcher3.matches(map));
    }

    @Test
    public void testNullValueMatchPartial() {
        doTestNullValueMatches(MatcherMode.PARTIAL_MATCH);
    }

    @Test
    public void testNullValueMatchContents() {
        doTestNullValueMatches(MatcherMode.CONTENTS_MATCH);
    }

    @Test
    public void testNullValueMatchExact() {
        doTestNullValueMatches(MatcherMode.EXACT_MATCH);
    }

    protected void doTestNullValueMatches(MatcherMode mode) {
        final Map<String, String> map = new HashMap<>();

        map.put("one", null);

        MapContentsMatcher<String, String> matcher = new MapContentsMatcher<>(mode);

        matcher.addExpectedEntry("one", null);

        assertTrue(matcher.matches(map));
    }

    @Test
    public void testMapEqualsWhenTheyAre() {
        final Map<String, String> map = new HashMap<>();

        map.put("one", "1");
        map.put("two", "2");
        map.put("three", "3");

        MapContentsMatcher<String, String> matcher = new MapContentsMatcher<>(MatcherMode.CONTENTS_MATCH);

        matcher.addExpectedEntry("one", "1");
        matcher.addExpectedEntry("two", "2");
        matcher.addExpectedEntry("three", "3");

        assertTrue(matcher.matches(map));
    }

    @Test
    public void testMapEqualsWhenTheyAreNotForContensts() {
        final Map<String, String> map = new HashMap<>();

        map.put("one", "1");
        map.put("two", "2");
        map.put("three", "3");

        MapContentsMatcher<String, String> matcher = new MapContentsMatcher<>(MatcherMode.CONTENTS_MATCH);

        matcher.addExpectedEntry("one", "1");
        assertFalse(matcher.matches(map));

        matcher.addExpectedEntry("two", "2");
        assertFalse(matcher.matches(map));

        matcher.addExpectedEntry("three", "3");
        assertTrue(matcher.matches(map));  // finally equal
    }

    @Test
    public void testMapEqualsWhenTheyAreNotForContensts2() {
        final Map<String, String> map = new LinkedHashMap<>();

        map.put("one", "1");
        map.put("two", "2");
        map.put("three", "3");

        MapContentsMatcher<String, String> matcher = new MapContentsMatcher<>(MatcherMode.EXACT_MATCH);

        matcher.addExpectedEntry("one", "1");
        assertFalse(matcher.matches(map));

        matcher.addExpectedEntry("two", "2");
        assertFalse(matcher.matches(map));

        matcher.addExpectedEntry("three", "3");
        assertTrue(matcher.matches(map));  // finally equal
    }

    @Test
    public void testMapContentsMustBeInOrderForExactMatcher() {
        final Map<String, String> map = new LinkedHashMap<>();

        map.put("one", "1");
        map.put("three", "3");
        map.put("two", "2");

        MapContentsMatcher<String, String> matcher = new MapContentsMatcher<>(MatcherMode.EXACT_MATCH);

        matcher.addExpectedEntry("one", "1");
        matcher.addExpectedEntry("two", "2");
        matcher.addExpectedEntry("three", "3");

        assertFalse(matcher.matches(map));
    }

    @Test
    public void testMapEqualsWhenItContainsTheValueExpectedButAlsoOthers() {
        final Map<String, String> map = new HashMap<>();

        map.put("one", "1");
        map.put("two", "2");
        map.put("three", "3");
        map.put("four", "4");
        map.put("five", "5");

        MapContentsMatcher<String, String> matcher = new MapContentsMatcher<>(MatcherMode.PARTIAL_MATCH);

        matcher.addExpectedEntry("one", "1");
        matcher.addExpectedEntry("two", "2");
        matcher.addExpectedEntry("three", "3");

        assertTrue(matcher.matches(map));
    }

    @Test
    public void testExactMatchFailsWhenMoreElementsThanExpected() {
        final Map<String, String> map = new HashMap<>();

        map.put("one", "1");
        map.put("two", "2");
        map.put("three", "3");
        map.put("four", "4");
        map.put("five", "5");

        MapContentsMatcher<String, String> matcher = new MapContentsMatcher<>(MatcherMode.EXACT_MATCH);

        matcher.addExpectedEntry("one", "1");
        matcher.addExpectedEntry("two", "2");
        matcher.addExpectedEntry("three", "3");

        assertFalse(matcher.matches(map));
    }
}
