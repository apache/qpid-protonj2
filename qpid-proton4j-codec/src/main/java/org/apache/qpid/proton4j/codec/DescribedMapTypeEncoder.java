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
package org.apache.qpid.proton4j.codec;

import java.util.Map;
import java.util.Set;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Base class used for all Described Type objects that are represented as a List
 *
 * @param <M> the map type that is being encoded.
 * @param <K> the key type used for the encoded map's keys.
 * @param <V> the value type used for the encoded map's values.
 */
public interface DescribedMapTypeEncoder<K, V, M> extends DescribedTypeEncoder<M> {

    /**
     * Returns the encoding code of the largest encoding this Map type can be
     * encoded to.
     * <p>
     * Most encoders will return MAP32 but for cases where the type is know to
     * be encoded to MAP8 the encode process can skip size computations which
     * will increase encoder performance.
     *
     * @return the encoding code of the largest map type needed for this object.
     */
    default int getLargestEncoding() {
        return EncodingCodes.MAP32 & 0xff;
    }

    /**
     * Returns true when the value to be encoded has no Map body and can be
     * written as a Null body type instead of a Map type.
     *
     * @param value
     *		the value which be encoded as a map type.
     *
     * @return true if the type to be encoded has no Map body.
     */
    boolean hasMap(M value);

    /**
     * Gets the number of elements that will result when this type is encoded
     * into an AMQP Map type.
     *
     * @param value
     * 		the value which will be encoded as a map type.
     *
     * @return the number of elements that should comprise the encoded list.
     */
    int getMapSize(M value);

    /**
     * Gets the Map entries that comprise the Map that is to be encoded.
     *
     * @param value
     * 		the value which will be encoded as a map type.
     *
     * @return the Map entries that are to be encoded.
     */
    Set<Map.Entry<K, V>> getMapEntries(M value);

    @Override
    default void writeValue(ProtonBuffer buffer, EncoderState state, M value) {
        if (!hasMap(value)) {
            state.getEncoder().writeNull(buffer, state);
            return;
        }

        int count = getMapSize(value);
        int encodingCode = getLargestEncoding();

        final int fieldWidth;

        if (encodingCode == EncodingCodes.MAP8) {
            fieldWidth = 1;
            buffer.writeByte(EncodingCodes.MAP8);
        } else {
            fieldWidth = 4;
            buffer.writeByte(EncodingCodes.MAP32);
        }

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        if (fieldWidth == 1) {
            buffer.writeByte((byte) 0);
            buffer.writeByte((byte) (count * 2));
        } else {
            buffer.writeInt(0);
            buffer.writeInt(count * 2);
        }

        Set<Map.Entry<K, V>> entrySet = getMapEntries(value);

        // Write the Map elements and then compute total size written.
        for (Map.Entry<K, V> entry : entrySet) {
            state.getEncoder().writeObject(buffer, state, entry.getKey());
            state.getEncoder().writeObject(buffer, state, entry.getValue());
        }

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        int writeSize = endIndex - startIndex - fieldWidth;

        if (fieldWidth == 1) {
            buffer.setByte(startIndex, writeSize);
        } else {
            buffer.setInt(startIndex, writeSize);
        }
    }
}
