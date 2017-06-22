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
package org.apache.qpid.proton4j.amqp;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class Symbol implements Comparable<Symbol>, CharSequence {

    private static final ConcurrentMap<String, Symbol> symbols = new ConcurrentHashMap<String, Symbol>(2048);

    private final String underlying;

    private Symbol(String underlying) {
        this.underlying = underlying;
    }

    @Override
    public int length() {
        return underlying.length();
    }

    @Override
    public int compareTo(Symbol o) {
        return underlying.compareTo(o.underlying);
    }

    @Override
    public char charAt(int index) {
        return underlying.charAt(index);
    }

    @Override
    public CharSequence subSequence(int beginIndex, int endIndex) {
        return underlying.subSequence(beginIndex, endIndex);
    }

    @Override
    public String toString() {
        return underlying;
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }

    public static Symbol valueOf(String symbolVal) {
        return getSymbol(symbolVal);
    }

    public static Symbol getSymbol(String symbolVal) {
        if (symbolVal == null) {
            return null;
        }

        Symbol symbol = symbols.get(symbolVal);

        if (symbol == null) {
            symbolVal = symbolVal.intern();
            symbol = new Symbol(symbolVal);
            Symbol existing;
            if ((existing = symbols.putIfAbsent(symbolVal, symbol)) != null) {
                symbol = existing;
            }
        }

        return symbol;
    }
}
