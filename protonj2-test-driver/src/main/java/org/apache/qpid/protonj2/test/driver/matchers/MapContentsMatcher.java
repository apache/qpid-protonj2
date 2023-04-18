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

package org.apache.qpid.protonj2.test.driver.matchers;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher used to compare Map instance either for full or partial contents
 * matches.
 *
 * @param <K> The key type used to define the Map entry.
 * @param <V> The value type used to define the Map entry.
 */
public class MapContentsMatcher<K, V> extends TypeSafeMatcher<Map<K, V>> {

    public enum MatcherMode {
        /**
         * As long as all the added expected entries match the Map is considered
         * to be matching.
         */
        PARTIAL_MATCH,
        /**
         * As long as both Maps have the same contents (and size) the Map is
         * considered to be matching.
         */
        CONTENTS_MATCH,
        /**
         * The contents and size of the Map must match the expectations but
         * also the order of Map entries iterated over must match the order
         * of the expectations as they were added.
         */
        EXACT_MATCH
    }

    private final Map<K, V> expectedContents = new LinkedHashMap<>();

    private MatcherMode mode;
    private String mismatchDescription;

    /**
     * Creates a matcher that matches if any of the expected entries is found
     */
    public MapContentsMatcher() {
        this(MatcherMode.PARTIAL_MATCH);
    }

    /**
     * Creates a matcher that matches if the expected entries are the only entries
     * in the Map but doesn't check order.
     *
     * @param entries
     * 		The entries that must be matched.
     */
    public MapContentsMatcher(Map<K, V> entries) {
        this(MatcherMode.CONTENTS_MATCH);

        entries.forEach((k, v) -> addExpectedEntry(k, v));
    }

    /**
     * Creates a matcher that matches if the expected entries are the only entries
     * in the Map but doesn't check order.
     *
     * @param entries
     * 		The entries that must be matched.
     * @param strictOrder
     * 		Controls if order also considered when matching.
     */
    public MapContentsMatcher(Map<K, V> entries, boolean strictOrder) {
        this(strictOrder ? MatcherMode.EXACT_MATCH : MatcherMode.CONTENTS_MATCH);

        entries.forEach((k, v) -> addExpectedEntry(k, v));
    }

    /**
     * Creates a new {@link Map} contents matcher with the given strict setting.
     * <p>
     * When in strict mode the contents must match both in the entry values and the
     * number of entries expected vs the number of entries in the given {@link Map}.
     *
     * @param mode
     * 		The matcher mode to use when performing the match.
     */
    public MapContentsMatcher(MatcherMode mode) {
        this.mode = mode;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(mismatchDescription);
    }

    @Override
    protected boolean matchesSafely(Map<K, V> item) {
        switch (mode) {
            case CONTENTS_MATCH:
                return performContentsOnlyMatch(item);
            case EXACT_MATCH:
                return performInOrderContentsMatch(item);
            case PARTIAL_MATCH:
            default:
                return performPartialMatch(item);
        }
    }

    private boolean performMapInvariantsCheck(Map<K, V> map) {
        if (map.isEmpty() && !expectedContents.isEmpty()) {
            mismatchDescription = String.format("Expecting an empty map but got a map of size %s instead", expectedContents.size());
            return false;
        } else if (!map.isEmpty() && expectedContents.isEmpty()) {
            mismatchDescription = String.format("Expecting map of size %s but got an empty map instead", expectedContents.size());
            return false;
        } else if (map.size() != expectedContents.size()) {
            mismatchDescription = String.format("Expecting map with %s items but got a map of size %s instead",
                                                expectedContents.size(), map.size());
            return false;
        } else {
            return true;
        }
    }

    private boolean performInOrderContentsMatch(Map<K, V> map) {
        if (!performMapInvariantsCheck(map)) {
            return false;
        }

        final Iterator<Entry<K, V>> mapIterator = map.entrySet().iterator();

        for (Entry<K, V> entry : expectedContents.entrySet()) {
             Entry<K, V> mapEntry = mapIterator.next();

            if (!entry.getKey().equals(mapEntry.getKey())) {
                mismatchDescription = String.format(
                    "Expected to find a key matching %s but got %s", entry.getKey(), mapEntry.getKey());
                return false;
            }

            if (entry.getValue() == null && mapEntry.getValue() == null) {
                continue;
            }

            if (!entry.getValue().equals(mapEntry.getValue())) {
                mismatchDescription = String.format(
                    "Expected to find a value matching %s for key %s but got %s",
                    entry.getKey(), entry.getValue(), mapEntry.getKey());
                return false;
            }
        }

        return true;
    }

    private boolean performContentsOnlyMatch(Map<K, V> map) {
        if (!performMapInvariantsCheck(map)) {
            return false;
        }

        for (Entry<K, V> entry : expectedContents.entrySet()) {
            if (!map.containsKey(entry.getKey())) {
                mismatchDescription = String.format(
                    "Expected to find a key matching %s but it wasn't found in the Map", entry.getKey());
                return false;
            } else if (!Objects.equals(entry.getValue(), map.get(entry.getKey()))) {
                mismatchDescription = String.format(
                    "Expected to find a value matching %s for key %s but got %s",
                    entry.getKey(), entry.getValue(), map.get(entry.getKey()));
                return false;
            }
        }

        return true;
    }

    private boolean performPartialMatch(Map<K, V> map) {
        for (Entry<K, V> entry : expectedContents.entrySet()) {
            if (!map.containsKey(entry.getKey())) {
                mismatchDescription = String.format(
                    "Expected to find a key matching %s but it wasn't found in the Map", entry.getKey());
                return false;
            } else if (!Objects.equals(entry.getValue(), map.get(entry.getKey()))) {
                mismatchDescription = String.format(
                    "Expected to find a value matching %s for key %s but got %s",
                    entry.getKey(), entry.getValue(), map.get(entry.getKey()));
                return false;
            }
        }

        return true;
    }

    public void addExpectedEntry(K key, V value) {
        expectedContents.put(key, value);
    }

    /**
     * @return true if the Maps must match as equal or if only some contents need to match.
     */
    public boolean isStrictEaulityMatching() {
        return mode.ordinal() > MatcherMode.PARTIAL_MATCH.ordinal();
    }
}
