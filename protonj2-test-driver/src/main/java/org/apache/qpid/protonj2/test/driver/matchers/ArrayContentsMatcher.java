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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher used to compare Array instance either for full or partial contents
 * matches.
 *
 * @param <E> The value type used to define the Array entry.
 */
public class ArrayContentsMatcher<E> extends TypeSafeMatcher<E[]> {

    public enum MatcherMode {
        /**
         * As long as all the added expected entries match the array is considered
         * to be matching.
         */
        PARTIAL_MATCH,
        /**
         * As long as both Arrays have the same contents (and size) the Array is
         * considered to be matching.
         */
        CONTENTS_MATCH,
        /**
         * The contents and size of the Array must match the expectations but
         * also the order of Array entries iterated over must match the order
         * of the expectations as they were added.
         */
        EXACT_MATCH
    }

    private final List<E> expectedContents = new ArrayList<>();

    private MatcherMode mode;
    private String mismatchDescription;

    /**
     * Creates a matcher that matches if any of the expected entries is found
     */
    public ArrayContentsMatcher() {
        this(MatcherMode.PARTIAL_MATCH);
    }

    /**
     * Creates a matcher that matches if the expected entries are the only entries
     * in the array but doesn't check order.
     *
     * @param entries
     * 		The entries that must be matched.
     */
    public ArrayContentsMatcher(Collection<E> entries) {
        this(MatcherMode.CONTENTS_MATCH);

        entries.forEach((e) -> addExpectedEntry(e));
    }

    /**
     * Creates a matcher that matches if the expected entries are the only entries
     * in the array but doesn't check order.
     *
     * @param entries
     * 		The entries that must be matched.
     * @param strictOrder
     * 		Controls if order also considered when matching.
     */
    public ArrayContentsMatcher(Collection<E> entries, boolean strictOrder) {
        this(strictOrder ? MatcherMode.EXACT_MATCH : MatcherMode.CONTENTS_MATCH);

        entries.forEach((e) -> addExpectedEntry(e));
    }

    /**
     * Creates a new array contents matcher with the given strict setting.
     * <p>
     * When in strict mode the contents must match both in the entry values and the
     * number of entries expected vs the number of entries in the given array.
     *
     * @param mode
     * 		The matcher mode to use when performing the match.
     */
    public ArrayContentsMatcher(MatcherMode mode) {
        this.mode = mode;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(mismatchDescription);
    }

    @Override
    protected boolean matchesSafely(E[] array) {
        switch (mode) {
            case CONTENTS_MATCH:
                return performContentsOnlyMatch(array);
            case EXACT_MATCH:
                return performInOrderContentsMatch(array);
            case PARTIAL_MATCH:
            default:
                return performPartialMatch(array);
        }
    }

    private boolean performArrayInvariantsCheck(E[] array) {
        if (array.length == 0 && !expectedContents.isEmpty()) {
            mismatchDescription = String.format("Expecting an empty array but got an array of size %s instead", expectedContents.size());
            return false;
        } else if (array.length > 0 && expectedContents.isEmpty()) {
            mismatchDescription = String.format("Expecting array of size %s but got an empty array instead", expectedContents.size());
            return false;
        } else if (array.length != expectedContents.size()) {
            mismatchDescription = String.format("Expecting array with %s items but got a array of size %s instead",
                                                expectedContents.size(), array.length);
            return false;
        } else {
            return true;
        }
    }

    private boolean performInOrderContentsMatch(E[] array) {
        if (!performArrayInvariantsCheck(array)) {
            return false;
        }

        final List<E> elements = Arrays.asList(array);
        final Iterator<E> elementsIterator = elements.iterator();

        for (E expectedEntry : expectedContents) {
             E arrayEntry = elementsIterator.next();

            if (!Objects.equals(expectedEntry, arrayEntry)) {
                mismatchDescription = String.format(
                    "Expected to find a value matching %s but got %s", expectedEntry, arrayEntry);
                return false;
            }
        }

        return true;
    }

    private boolean performContentsOnlyMatch(E[] array) {
        if (!performArrayInvariantsCheck(array)) {
            return false;
        }

        return performPartialMatch(array);
    }

    private boolean performPartialMatch(E[] array) {
        final List<E> elements = new ArrayList<>(Arrays.asList(array));

        for (E expectedEntry : expectedContents) {
            final int index = elements.indexOf(expectedEntry);

            if (index < 0) {
                mismatchDescription = String.format("Expected to find an entry matching %s but it wasn't found in the array", expectedEntry);
                return false;
            }

            // Remove the found item, if a duplicate value is expected it must also exist and not match on this one.
            elements.remove(index);
        }

        return true;
    }

    public void addExpectedEntry(E value) {
        expectedContents.add(value);
    }

    /**
     * @return true if the Arrays must match as equal or if only some contents need to match.
     */
    public boolean isStrictEaulityMatching() {
        return mode.ordinal() > MatcherMode.PARTIAL_MATCH.ordinal();
    }
}
