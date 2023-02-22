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

package org.apache.qpid.protonj2.types;

import java.util.Objects;

import org.apache.qpid.protonj2.engine.util.StringUtils;

/**
 * This class provides a set of APIs for interacting with sets of Symbol values.
 */
public final class Symbols {

    private Symbols() {
        // No need to create
    }

    /**
     * Look up a set of {@link Symbol} instances that matches the given {@link String}
     * array names of the {@link Symbol} values and return them as a new {@link Symbol}
     * array.
     *
     * @param stringValues
     * 		The {@link String} array version of the {@link Symbol} values.
     *
     * @return a {@link Symbol} array that matches the given {@link String} array values.
     */
    public static Symbol[] getSymbols(String[] stringValues) {
        return StringUtils.toSymbolArray(stringValues);
    }

    /**
     * Given an array of {@link Symbol} values search for a specific value and
     * return <code>true</code> if the value is found in the array.
     *
     * @param symbols
     * 		The array of symbols that should be searched (can be null or empty).
     * @param value
     * 		The value to search for in the provided array (cannot be null).
     *
     * @return true if the value is found in the symbol array.
     */
    public static boolean contains(Symbol[] symbols, String value) {
        return contains(symbols, Symbol.getSymbol(value));
    }

    /**
     * Given an array of {@link Symbol} values search for a specific value and
     * return <code>true</code> if the value is found in the array.
     *
     * @param symbols
     * 		The array of symbols that should be searched (can be null or empty).
     * @param value
     * 		The value to search for in the provided array (cannot be null).
     *
     * @return true if the value is found in the symbol array.
     */
    public static boolean contains(Symbol[] symbols, Symbol value) {
        Objects.requireNonNull("Value to find cannot be null");

        if (symbols == null || symbols.length == 0) {
            return false;
        }

        for (Symbol symbol : symbols) {
            if (value.equals(symbol)) {
                return true;
            }
        }

        return false;
    }
}
