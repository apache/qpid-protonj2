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
package org.messaginghub.amqperative.impl;

/**
 * Simple utility for generating tags for transfers
 */
public final class TransferTagGenerator {

    private long nextTagId;

    /**
     * Retrieves the next tag in the sequence of tags.
     *
     * @return a new tag value.
     */
    public byte[] getNextTag() {
        long tag = nextTagId++;
        int size = encodingSize(tag);

        byte[] tagBytes = new byte[size];

        for (int i = 0; i < size; ++i) {
            tagBytes[size - 1 - i] = (byte) (tag >>> (i * 8));
        }

        return tagBytes;
    }

    private int encodingSize(long value) {
        if (value < 0) {
            return Long.BYTES;
        }

        int size = 1;
        while (size < 8 && (value >= (1L << (size * 8)))) {
            size++;
        }

        return size;
    }
}
