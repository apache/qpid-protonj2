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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;

public final class Symbol implements Comparable<Symbol> {

    private static final Map<ProtonBuffer, Symbol> bufferToSymbols = new ConcurrentHashMap<>(2048);

    private static final Symbol EMPTY_SYMBOL = new Symbol();

    private final ProtonBuffer underlying;
    private final int hashCode;

    private Symbol() {
        this.underlying = ProtonByteBufferAllocator.DEFAULT.allocate(0, 0);
        this.hashCode = 31;
    }

    private Symbol(ProtonBuffer underlying) {
        this.underlying = underlying;
        this.hashCode = underlying.hashCode();
    }

    public int getLength() {
        return underlying.getReadableBytes();
    }

    @Override
    public int compareTo(Symbol other) {
        return underlying.compareTo(other.underlying);
    }

    @Override
    public String toString() {
        return underlying.toString(StandardCharsets.US_ASCII);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    public void writeTo(ProtonBuffer target) {
        target.writeBytes(underlying, 0, underlying.getReadableBytes());
    }

    public static Symbol valueOf(String symbolVal) {
        return getSymbol(symbolVal);
    }

    public static Symbol getSymbol(ProtonBuffer symbolBytes) {
        return getSymbol(symbolBytes, false);
    }

    public static Symbol getSymbol(ProtonBuffer symbolBuffer, boolean copyOnCreate) {
        if (symbolBuffer == null) {
            return null;
        } else if (symbolBuffer.getReadableBytes() == 0) {
            return EMPTY_SYMBOL;
        }

        Symbol symbol = bufferToSymbols.get(symbolBuffer);
        if (symbol == null) {
            if (copyOnCreate) {
                symbolBuffer = symbolBuffer.copy();
            }

            symbol = new Symbol(symbolBuffer);
            Symbol existing;
            if ((existing = bufferToSymbols.putIfAbsent(symbolBuffer, symbol)) != null) {
                symbol = existing;
            }
        }

        return symbol;
    }

    public static Symbol getSymbol(String symbolVal) {
        if (symbolVal == null) {
            return null;
        } else if (symbolVal.isEmpty()) {
            return EMPTY_SYMBOL;
        }

        byte[] symbolBytes = symbolVal.getBytes(StandardCharsets.US_ASCII);

        return getSymbol(ProtonByteBufferAllocator.DEFAULT.wrap(symbolBytes));
    }
}
