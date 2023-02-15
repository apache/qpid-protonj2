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

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.apache.qpid.protonj2.engine.util.StringUtils;

/**
 * Class that represents an AMQP Symbol value.  The creation of a Symbol object
 * occurs during a lookup operation which cannot find an already stored version
 * of the string or byte buffer view of the Symbol's ASCII bytes.
 */
public final class Symbol implements Comparable<Symbol> {

    private static final Map<ProtonBuffer, Symbol> bufferToSymbols = new ConcurrentHashMap<>(2048);
    private static final Map<String, Symbol> stringToSymbols = new ConcurrentHashMap<>(2048);

    private static final Symbol EMPTY_SYMBOL = new Symbol();

    private static final int MAX_CACHED_SYMBOL_SIZE = 64;

    private String symbolString;
    private final ProtonBuffer underlying;
    private final int hashCode;

    private Symbol() {
        this.underlying = ProtonBufferAllocator.defaultAllocator().allocate(0).convertToReadOnly();
        this.hashCode = 31;
        this.symbolString = "";
    }

    private Symbol(ProtonBuffer underlying) {
        this.underlying = underlying;
        this.hashCode = underlying.hashCode();
    }

    /**
     * @return the number of bytes that comprise the Symbol value.
     */
    public int getLength() {
        return underlying.getReadableBytes();
    }

    @Override
    public int compareTo(Symbol other) {
        return underlying.compareTo(other.underlying);
    }

    @Override
    public String toString() {
        if (symbolString == null && underlying.getReadableBytes() > 0) {
            symbolString = underlying.toString(US_ASCII);

            if (underlying.getReadableBytes() <= MAX_CACHED_SYMBOL_SIZE) {
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

    /**
     * Writes the internal {@link Symbol} bytes to the provided {@link ProtonBuffer}.  This
     * is a raw ASCII encoding of the Symbol without and AMQP type encoding.
     *
     * @param target
     * 		The buffer where the Symbol bytes should be written to.
     */
    public void writeTo(ProtonBuffer target) {
        target.ensureWritable(underlying.getReadableBytes());
        underlying.copyInto(underlying.getReadOffset(), target, target.getWriteOffset(), underlying.getReadableBytes());
        target.advanceWriteOffset(underlying.getReadableBytes());
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link String}
     * name of the {@link Symbol}.
     *
     * @param symbolVal
     * 		The {@link String} version of the {@link Symbol} value.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol valueOf(String symbolVal) {
        return getSymbol(symbolVal);
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link ProtonBuffer}
     * byte view of the {@link Symbol}.
     *
     * @param symbolBytes
     * 		The {@link String} version of the {@link Symbol} value.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSymbol(ProtonBuffer symbolBytes) {
        return getSymbol(symbolBytes, false);
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link ProtonBuffer}
     * byte view of the {@link Symbol}.
     *
     * @param symbolBuffer
     * 		The {@link ProtonBuffer} version of the {@link Symbol} value.
     * @param copyOnCreate
     * 		Should the provided buffer be copied during creation of a new {@link Symbol}.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSymbol(ProtonBuffer symbolBuffer, boolean copyOnCreate) {
        if (symbolBuffer == null) {
            return null;
        } else if (symbolBuffer.getReadableBytes() == 0) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = bufferToSymbols.get(symbolBuffer);
        if (symbol == null) {
            if (copyOnCreate) {
                // Copy to a known heap based buffer to avoid issue with life-cycle of pooled buffer types.
                int symbolSize = symbolBuffer.getReadableBytes();
                ProtonBuffer copy = ProtonBufferAllocator.defaultAllocator().allocate(symbolSize);
                copy.writeBytes(symbolBuffer);
                symbolBuffer = copy.convertToReadOnly();
            }

            symbol = new Symbol(symbolBuffer);

            // Don't cache overly large symbols to prevent holding large
            // amount of memory in the symbol cache.
            if (symbolBuffer.getReadableBytes() <= MAX_CACHED_SYMBOL_SIZE) {
                final Symbol existing;
                if ((existing = bufferToSymbols.putIfAbsent(symbolBuffer, symbol)) != null) {
                    symbol = existing;
                }
            }
        }

        return symbol;
    }

    /**
     * Look up a singleton {@link Symbol} instance that matches the given {@link String}
     * name of the {@link Symbol}.
     *
     * @param stringValue
     * 		The {@link String} version of the {@link Symbol} value.
     *
     * @return a {@link Symbol} that matches the given {@link String}.
     */
    public static Symbol getSymbol(String stringValue) {
        if (stringValue == null) {
            return null;
        } else if (stringValue.isEmpty()) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = stringToSymbols.get(stringValue);
        if (symbol == null) {
            symbol = getSymbol(ProtonBufferAllocator.defaultAllocator().copy(stringValue.getBytes(US_ASCII)));

            // Don't cache overly large symbols to prevent holding large
            // amount of memory in the symbol cache.
            if (symbol.underlying.getReadableBytes() <= MAX_CACHED_SYMBOL_SIZE) {
                stringToSymbols.put(stringValue, symbol);
            }
        }

        return symbol;
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
}
