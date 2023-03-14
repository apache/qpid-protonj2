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

package org.apache.qpid.protonj2.codec.decoders;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferUtils;
import org.apache.qpid.protonj2.buffer.impl.ProtonByteArrayBufferAllocator;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.types.Symbol;

public class ProtonScanningContext<Type> implements ScanningContext<Type>, StreamScanningContext<Type> {

    private final Class<?> expectedType;
    private final List<Type> entries;
    private final List<ProtonBuffer> encodedEntries;
    private final boolean[] matched;

    private int matches;

    public ProtonScanningContext(Class<?> expectedType, Type entry, ProtonBuffer encodedEntry) {
        this.expectedType = expectedType;
        this.entries = Collections.singletonList(entry);
        this.encodedEntries = Collections.singletonList(encodedEntry);
        this.matched = new boolean[entries.size()];

        Arrays.fill(matched, false);
    }

    public ProtonScanningContext(Class<?> expectedType, List<Type> entries, List<ProtonBuffer> encodedEntries) {
        this.expectedType = expectedType;
        this.entries = entries;
        this.encodedEntries = encodedEntries;
        this.matched = new boolean[entries.size()];

        Arrays.fill(matched, false);
    }

    @Override
    public void reset() {
        matches = 0;
        Arrays.fill(matched, false);
    }

    @Override
    public boolean isComplete() {
        return matches == matched.length;
    }

    @Override
    public boolean matches(TypeDecoder<?> typeDecoder, ProtonBuffer candidate, int candidateLength, Consumer<Type> matchConsumer) {
        if (isComplete() || !expectedType.equals(typeDecoder.getTypeClass())) {
            return false;
        }

        for (int i = 0; i < matched.length; ++i) {
            if (matched[i]) {
                continue;
            }

            if (ProtonBufferUtils.equals(encodedEntries.get(i), 0, candidate, candidate.getReadOffset(), candidateLength)) {
                matched[i] = true;
                if (matchConsumer != null) {
                    matchConsumer.accept(entries.get(i));
                }

                return true;
            }
        }

        return false;
    }

    @Override
    public boolean matches(StreamTypeDecoder<?> typeDecoder, InputStream stream, int encodedSize, Consumer<Type> matchConsumer) {
        if (!stream.markSupported()) {
            throw new UnsupportedOperationException("Type scanner requires a stream with mark and reset support");
        }

        if (isComplete() || !expectedType.equals(typeDecoder.getTypeClass())) {
            return false;
        }

        try {
            for (int i = 0; i < matched.length; ++i) {
                if (matched[i]) {
                    continue;
                }

                final ProtonBuffer candidate = encodedEntries.get(i);

                boolean matchFound = true;

                if (candidate.getReadableBytes() == encodedSize) {
                    stream.mark(encodedSize);
                    try {
                        for (int j = 0; j < encodedSize; ++j) {
                            if (candidate.getByte(j) != stream.read()) {
                                matchFound = false;
                                break;
                            }
                        }
                    } finally {
                        stream.reset();
                    }

                    if (matchFound) {
                        matched[i] = true;
                        if (matchConsumer != null) {
                            matchConsumer.accept(entries.get(i));
                        }

                        return true;
                    }
                }
            }
        } catch (IOException ex) {
            throw new DecodeException("Error while scanning input stream", ex);
        }

        return false;
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param entries
     * 		The {@link String} entries that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link StreamScanningContext} for the collection of string entries.
     */
    public static ProtonScanningContext<String> createStringScanContext(String...entries) {
        Objects.requireNonNull(entries, "The entries to search for cannot be null");

        return internalCreateStringScanContext(Arrays.asList(entries));
    }

    /**
     * Creates a new scanning context for the given collection of {@link String} values.
     *
     * @param entries
     * 		The {@link String} entries that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link StreamScanningContext} for the collection of string entries.
     */
    public static ProtonScanningContext<String> createStringScanContext(Collection<String> entries) {
        Objects.requireNonNull(entries, "The entries to search for cannot be null");

        return internalCreateStringScanContext(new ArrayList<>(entries));
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param entries
     * 		The {@link Symbol} entries that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link StreamScanningContext} for the collection of symbol entries.
     */
    public static ProtonScanningContext<Symbol> createSymbolScanContext(Symbol...entries) {
        Objects.requireNonNull(entries, "The entries to search for cannot be null");

        return internalCreateSymbolScanContext(Arrays.asList(entries));
    }

    /**
     * Creates a new scanning context for the given collection of {@link Symbol} values.
     *
     * @param entries
     * 		The {@link Symbol} entries that will be scanned for in the encoded {@link Map}
     *
     * @return a {@link StreamScanningContext} for the collection of symbol entries.
     */
    public static ProtonScanningContext<Symbol> createSymbolScanContext(Collection<Symbol> entries) {
        Objects.requireNonNull(entries, "The entries to search for cannot be null");

        return internalCreateSymbolScanContext(new ArrayList<>(entries));
    }

    private static ProtonScanningContext<String> internalCreateStringScanContext(List<String> entries) {
        Objects.requireNonNull(entries, "The entries to search for cannot be null");

        final List<ProtonBuffer> encodedentries = new ArrayList<>(entries.size());

        entries.forEach((key) ->
            encodedentries.add(ProtonByteArrayBufferAllocator.wrapped(key.getBytes(StandardCharsets.UTF_8)).convertToReadOnly()));

        return new ProtonScanningContext<String>(String.class, entries, encodedentries);
    }

    private static ProtonScanningContext<Symbol> internalCreateSymbolScanContext(List<Symbol> entries) {
        Objects.requireNonNull(entries, "The entries to search for cannot be null");

        final List<ProtonBuffer> encodedentries = new ArrayList<>(entries.size());

        entries.forEach((key) -> encodedentries.add(key.toASCII()));

        return new ProtonScanningContext<Symbol>(Symbol.class, entries, encodedentries);
    }
}
