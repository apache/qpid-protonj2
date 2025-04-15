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

import org.apache.qpid.protonj2.test.driver.matchers.ArrayContentsMatcher;
import org.apache.qpid.protonj2.test.driver.matchers.ArrayContentsMatcher.MatcherMode;
import org.junit.jupiter.api.Test;

/**
 * Test custom array contents matcher
 */
public class ArrayContainsMatcherTest {

    @Test
    public void testEmptyArraysAreEqualInAllModes() {
        final String[] array = new String[0];

        ArrayContentsMatcher<String> matcher1 = new ArrayContentsMatcher<>(MatcherMode.PARTIAL_MATCH);
        ArrayContentsMatcher<String> matcher2 = new ArrayContentsMatcher<>(MatcherMode.CONTENTS_MATCH);
        ArrayContentsMatcher<String> matcher3 = new ArrayContentsMatcher<>(MatcherMode.EXACT_MATCH);

        assertTrue(matcher1.matches(array));
        assertTrue(matcher2.matches(array));
        assertTrue(matcher3.matches(array));
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
        final String[] array = new String[1];

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(mode);

        matcher.addExpectedEntry(null);

        assertTrue(matcher.matches(array));
    }

    @Test
    public void testNullValueMissingMatchPartial() {
        doTestNullValueMatches(MatcherMode.PARTIAL_MATCH);
    }

    @Test
    public void testNullValueMissingMatchContents() {
        doTestNullValueMatches(MatcherMode.CONTENTS_MATCH);
    }

    @Test
    public void testNullValueMissingMatchExact() {
        doTestNullValueMatches(MatcherMode.EXACT_MATCH);
    }

    protected void doTestNullValueMissingMatches(MatcherMode mode) {
        final String[] array = new String[1];

        array[0] = "A";

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(mode);

        matcher.addExpectedEntry(null);

        assertFalse(matcher.matches(array));
    }

    @Test
    public void testArrayEqualsWhenTheyAre() {
        final String[] array = new String[3];

        array[0] = "A";
        array[1] = "B";
        array[2] = "C";

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(MatcherMode.EXACT_MATCH);

        matcher.addExpectedEntry("A");
        matcher.addExpectedEntry("B");
        matcher.addExpectedEntry("C");

        assertTrue(matcher.matches(array));

        matcher.addExpectedEntry("D");

        assertFalse(matcher.matches(array));
    }

    @Test
    public void testArrayEqualsWhenTheyAreNotForContents() {
        final String[] array = new String[3];

        array[0] = "A";
        array[1] = "B";
        array[2] = "C";

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(MatcherMode.CONTENTS_MATCH);

        matcher.addExpectedEntry("A");
        assertFalse(matcher.matches(array));

        matcher.addExpectedEntry("C");
        assertFalse(matcher.matches(array));

        matcher.addExpectedEntry("B");
        assertTrue(matcher.matches(array));  // finally equal

        matcher.addExpectedEntry("D");
        assertFalse(matcher.matches(array));
    }

    @Test
    public void testArrayContentsMustBeInOrderForExactMatcher() {
        final String[] array = new String[3];

        array[0] = "A";
        array[1] = "C";
        array[2] = "B";

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(MatcherMode.EXACT_MATCH);

        matcher.addExpectedEntry("A");
        matcher.addExpectedEntry("B");
        matcher.addExpectedEntry("C");

        assertFalse(matcher.matches(array));

        array[1] = "B";
        array[2] = "C";

        assertTrue(matcher.matches(array));
    }

    @Test
    public void testArrayEqualsWhenItContainsTheValueExpectedButAlsoOthers() {
        final String[] array = new String[5];

        array[0] = "A";
        array[1] = "C";
        array[2] = "B";
        array[3] = "D";
        array[4] = "E";

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(MatcherMode.PARTIAL_MATCH);

        matcher.addExpectedEntry("A");
        matcher.addExpectedEntry("B");
        matcher.addExpectedEntry("C");

        assertTrue(matcher.matches(array));
    }

    @Test
    public void testExactMatchFailsWhenMoreElementsThanExpected() {
        final String[] array = new String[5];

        array[0] = "A";
        array[1] = "B";
        array[2] = "B";
        array[3] = "C";
        array[4] = "C";

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(MatcherMode.EXACT_MATCH);

        matcher.addExpectedEntry("A");
        matcher.addExpectedEntry("B");
        matcher.addExpectedEntry("C");

        assertFalse(matcher.matches(array));
    }

    @Test
    public void testPartialMatchNeedsAllRepeatedEntries() {
        final String[] array = new String[6];

        array[0] = "A";
        array[1] = "B";
        array[2] = "B";
        array[3] = "C";
        array[4] = "C";
        array[5] = "D";

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(MatcherMode.PARTIAL_MATCH);

        matcher.addExpectedEntry("B");
        matcher.addExpectedEntry("C");
        matcher.addExpectedEntry("B");
        matcher.addExpectedEntry("C");
        matcher.addExpectedEntry("D");

        assertTrue(matcher.matches(array));

        array[5] = "E";

        assertFalse(matcher.matches(array));
    }

    @Test
    public void testArrayNotLargeEnoughForContentsMatch() {
        final String[] array = new String[6];

        array[0] = "A";
        array[1] = "B";
        array[2] = "C";

        ArrayContentsMatcher<String> matcher = new ArrayContentsMatcher<>(MatcherMode.CONTENTS_MATCH);

        matcher.addExpectedEntry("A");
        matcher.addExpectedEntry("C");
        matcher.addExpectedEntry("B");
        matcher.addExpectedEntry("C");
        matcher.addExpectedEntry("D");

        assertFalse(matcher.matches(array));
    }
}
