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
     * Determine the map type the given value can be encoded to based on the number
     * of bytes that would be needed to hold the encoded form of the resulting list
     * entries.
     * <p>
     * Most encoders will return MAP32 but for cases where the type is known to
     * be encoded to MAP8 the encoder can optimize the encode step and not compute
     * sizes.
     *
     * @param value
     *      The value that is to be encoded.
     *
     * @return the encoding code of the map type encoding needed for this object.
     */
    default int getMapEncoding(M value) {
        return EncodingCodes.MAP32 & 0xff;
    }

    /**
     * Returns false when the value to be encoded has no Map body and can be
     * written as a Null body type instead of a Map type.
     *
     * @param value
     *		the value which be encoded as a map type.
     *
     * @return true if the type to be encoded has a Map body, false otherwise.
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
     * Performs the write of the Map entries to the given buffer, the caller
     * takes care of writing the Map preamble and tracking the final size of
     * the written elements of the Map.
     *
     * @param value
     * 		the value which will be encoded as a map type.
     *
     * @return the Map entries that are to be encoded.
     */
    void writeMapEntries(ProtonBuffer buffer, EncoderState state, M value);

    @Override
    default void writeType(ProtonBuffer buffer, EncoderState state, M value) {
        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        state.getEncoder().writeUnsignedLong(buffer, state, getDescriptorCode());

        if (!hasMap(value)) {
            state.getEncoder().writeNull(buffer, state);
            return;
        }

        int count = getMapSize(value);
        int encodingCode = getMapEncoding(value);

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

        writeMapEntries(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        int writeSize = endIndex - startIndex - fieldWidth;

        if (fieldWidth == 1) {
            buffer.setByte(startIndex, writeSize);
        } else {
            buffer.setInt(startIndex, writeSize);
        }
    }

    @Override
    default void writeArray(ProtonBuffer buffer, EncoderState state, M[] value) {
        // TODO
    }
}
