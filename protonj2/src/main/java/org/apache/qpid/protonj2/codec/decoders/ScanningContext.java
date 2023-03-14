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

import java.util.function.Consumer;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.TypeDecoder;

/**
 * A search context used when scanning encoded AMQP types for entries or sections that
 * match any of a predetermined set of possible values usually made up of type encodings
 * that would match the incoming encoded data.
 * <p>
 * A scanning context is typically created with both the encoded and unencoded form of
 * the value to be scanned for so that a call to the match function can provide an
 * callback that will be provided the value to be sought without the need to perform
 * a complete decode of the scanned data.
 * <p>
 * The context is a single threaded actor whose state can be modified during the
 * scanning cycle in order to optimize the number of comparisons done as the scan
 * progresses. The caller must ensure a context is not passed to scanning operations
 * that are occurring concurrently.
 *
 * @param <Type> The concrete type of the entries to be scanned.
 */
public interface ScanningContext<Type> {

    /**
     * Reset the context to its original state at the end of a complete scan
     * which should allow the context to be used again when a new scan is
     * started (e.g. {@link #isComplete()} should start returning false).
     */
    void reset();

    /**
     * Allows for the scanner to optimize reading of encoded data by determining if
     * the target of the matching context has been found in which case the scanner
     * can consume any remaining encoded bytes without regard for the matcher. In
     * this state any call to {@link #matches(TypeDecoder, ProtonBuffer, int, Consumer)}
     * should return false as the target has already been found.
     *
     * @return true if the target of the matching context has already been found.
     */
    boolean isComplete();

    /**
     * Returns true if the encoded entry bytes match against the search domain
     * of the scan matching context and calls the provided match {@link Consumer}
     * with the original unencoded form of the matched entry. The caller must
     * provide the size of the encoded value being checked to allow for pass of
     * the source bytes without copying which could contain more entries following
     * the candidate value in question.
     * <p>
     * The matcher must not alter the read offset of the provided buffer, doing so
     * can corrupt the buffer state and likely cause decode exceptions on follow
     * on decode operations.
     *
     * @param typeDecoder
     *		The type decoder of the encoded type that is contained in the buffer.
     * @param candidate
     * 		Buffer whose first read index is the start of the encoded bytes.
     * @param candidateLength
     *      The region of the candidate buffer that contains the encoded bytes
     * @param matchConsumer
     * 		An optional consumer that should be called if a match is found.
     *
     * @return true if the candidate matches a target in the search domain.
     */
    boolean matches(TypeDecoder<?> typeDecoder, ProtonBuffer candidate, int candidateLength, Consumer<Type> matchConsumer);

}