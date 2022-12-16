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

package org.apache.qpid.protonj2.buffer;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An {@link Iterator} like API for accessing the underlying bytes of the
 * target buffer one at a time.
 */
public interface ProtonBufferIterator {

    /**
     * Called to check if more bytes are available from this iterator.
     *
     * @return true if there is another byte available or false if at the end of the buffer.
     */
    boolean hasNext();

    /**
     * Fetch the next byte from the buffer iterator.
     *
     * @return the next available byte from the buffer iterator.
     *
     * @throws NoSuchElementException if there is no next byte available.
     */
    byte next();

    /**
     * @return the number of remaining bytes that can be read from this iterator.
     */
    int remaining();

    /**
     * Returns the current offset into the buffer from where iteration started.
     *
     * @return the current offset into the buffer from where iteration started.
     */
    int offset();

    /**
     * Functional interface for defining standard or commonly used consumers of the
     * bytes provided by a {@link ProtonBufferIterator}.
     */
    @FunctionalInterface
    public interface ByteConsumer {

        /**
         * Consume one byte and return true if another should be provided or if
         * the consumer has finished or encountered some error during iteration.
         *
         * @param value
         * 		The byte value consumed from the source buffer iterator
         *
         * @return true if another byte should be provided or false if done.
         */
        boolean consume(byte value);
    }

    /**
     *
     * @param consumer
     * 		The consumer of the iterated bytes.
     *
     * @return the number of bytes that were iterated or -1 if iteration ended early.
     */
    default int forEach(ByteConsumer consumer) {
        int count = 0;

        while (hasNext()) {
            if (consumer.consume(next())) {
                count++;
            } else {
                return -1; // Consumer requested early exit
            }
        }

        return count;
    }
}
