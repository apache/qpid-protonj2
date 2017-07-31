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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class Symbol implements Comparable<Symbol> {

    private static final ConcurrentMap<String, Symbol> symbols = new ConcurrentHashMap<String, Symbol>(2048);

    private static final Symbol EMPTY_SYMBOL = new Symbol(new byte[0], "");

    private final byte[] underlying;
    private final String view;

    private Symbol(byte[] underlying, String view) {
        this.underlying = underlying;
        this.view = view;
    }

    @Override
    public int compareTo(Symbol o) {
        return view.compareTo(o.view);
    }

    @Override
    public String toString() {
        return view;
    }

    @Override
    public int hashCode() {
        return view.hashCode();
    }

    public byte[] getBytes() {
        return underlying;
    }

    public static Symbol valueOf(String symbolVal) {
        return getSymbol(symbolVal);
    }

    public static Symbol getSymbol(String symbolVal) {
        if (symbolVal == null) {
            return null;
        } else if (symbolVal.isEmpty()) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = symbols.get(symbolVal);

        if (symbol == null) {
            symbolVal = symbolVal.intern();
            symbol = new Symbol(symbolVal.getBytes(StandardCharsets.UTF_8), symbolVal);
            Symbol existing;
            if ((existing = symbols.putIfAbsent(symbolVal, symbol)) != null) {
                symbol = existing;
            }
        }

        return symbol;
    }
}
