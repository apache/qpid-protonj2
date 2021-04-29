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
package org.apache.qpid.protonj2.test.driver.codec.primitives;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Symbol implements Comparable<Symbol> {

    private static final Map<ByteBuffer, Symbol> bufferToSymbols = new ConcurrentHashMap<>(2048);
    private static final Map<String, Symbol> stringToSymbols = new ConcurrentHashMap<>(2048);

    private static final Symbol EMPTY_SYMBOL = new Symbol();

    private static final int MAX_CACHED_SYMBOL_SIZE = 64;

    private String symbolString;
    private final ByteBuffer underlying;
    private final int hashCode;

    private Symbol() {
        this.underlying = ByteBuffer.allocate(0);
        this.hashCode = 31;
        this.symbolString = "";
    }

    private Symbol(ByteBuffer underlying) {
        this.underlying = underlying;
        this.hashCode = underlying.hashCode();
    }

    public int getLength() {
        return underlying.remaining();
    }

    @Override
    public int compareTo(Symbol other) {
        return underlying.compareTo(other.underlying);
    }

    @Override
    public String toString() {
        if (symbolString == null && underlying.remaining() > 0) {
            symbolString = new String(underlying.array(), underlying.arrayOffset(), underlying.remaining(), StandardCharsets.US_ASCII);

            if (underlying.remaining() <= MAX_CACHED_SYMBOL_SIZE) {
                final Symbol existing;
                if ((existing = stringToSymbols.putIfAbsent(symbolString, this)) != null) {
                    symbolString = existing.symbolString;
                }
            }
        }

        return symbolString;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof Symbol) {
            return underlying.equals(((Symbol) other).underlying);
        }

        return false;
    }

    public void writeTo(ByteBuffer target) {
        for (int i = 0; i < underlying.remaining(); ++i) {
            target.put(underlying.get(i));
        }
    }

    public static Symbol valueOf(String symbolVal) {
        return getSymbol(symbolVal);
    }

    public static Symbol getSymbol(ByteBuffer symbolBytes) {
        return getSymbol(symbolBytes, false);
    }

    public static Symbol getSymbol(ByteBuffer symbolBuffer, boolean copyOnCreate) {
        if (symbolBuffer == null) {
            return null;
        } else if (!symbolBuffer.hasRemaining()) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = bufferToSymbols.get(symbolBuffer);
        if (symbol == null) {
            if (copyOnCreate) {
                // Copy to a known heap based buffer to avoid issue with life-cycle of pooled buffer types.
                int symbolSize = symbolBuffer.remaining();
                byte[] copy = new byte[symbolSize];
                symbolBuffer.get(copy, 0, symbolSize);
                symbolBuffer.position(symbolBuffer.position() - symbolSize);
                symbolBuffer = ByteBuffer.wrap(copy);
            }

            symbol = new Symbol(symbolBuffer);

            // Don't cache overly large symbols to prevent holding large
            // amount of memory in the symbol cache.
            if (symbolBuffer.remaining() <= MAX_CACHED_SYMBOL_SIZE) {
                final Symbol existing;
                if ((existing = bufferToSymbols.putIfAbsent(symbolBuffer, symbol)) != null) {
                    symbol = existing;
                }
            }
        }

        return symbol;
    }

    public static Symbol getSymbol(String stringValue) {
        if (stringValue == null) {
            return null;
        } else if (stringValue.isEmpty()) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = stringToSymbols.get(stringValue);
        if (symbol == null) {
            symbol = getSymbol(ByteBuffer.wrap(stringValue.getBytes(US_ASCII)));

            // Don't cache overly large symbols to prevent holding large
            // amount of memory in the symbol cache.
            if (symbol.underlying.remaining() <= MAX_CACHED_SYMBOL_SIZE) {
                stringToSymbols.put(stringValue, symbol);
            }
        }

        return symbol;
    }
}
