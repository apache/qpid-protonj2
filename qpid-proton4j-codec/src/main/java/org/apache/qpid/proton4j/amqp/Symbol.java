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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Symbol implements Comparable<Symbol> {

    private static final Map<String, Symbol> stringToSymbols = new ConcurrentHashMap<>(2048);
    private static final Map<ByteBuffer, Symbol> bufferToSymbols = new ConcurrentHashMap<>(2048);

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

    public static Symbol getSymbol(byte[] symbolBytes) {
        if (symbolBytes == null) {
            return null;
        } else if (symbolBytes.length == 0) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = bufferToSymbols.get(ByteBuffer.wrap(symbolBytes));

        if (symbol == null) {
            String symbolString = new String(symbolBytes, StandardCharsets.US_ASCII).intern();
            symbol = new Symbol(symbolBytes, symbolString);
            Symbol existing;
            if ((existing = stringToSymbols.putIfAbsent(symbolString, symbol)) != null) {
                symbol = existing;
            }

            bufferToSymbols.putIfAbsent(ByteBuffer.wrap(symbolBytes), symbol);
        }

        return symbol;
    }

    public static Symbol getSymbol(String symbolVal) {
        if (symbolVal == null) {
            return null;
        } else if (symbolVal.isEmpty()) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = stringToSymbols.get(symbolVal);

        if (symbol == null) {
            symbolVal = symbolVal.intern();
            byte[] symbolBytes = symbolVal.getBytes(StandardCharsets.UTF_8);
            symbol = new Symbol(symbolBytes, symbolVal);
            Symbol existing;
            if ((existing = stringToSymbols.putIfAbsent(symbolVal, symbol)) != null) {
                symbol = existing;
            }

            bufferToSymbols.putIfAbsent(ByteBuffer.wrap(symbolBytes), symbol);
        }

        return symbol;
    }
}
