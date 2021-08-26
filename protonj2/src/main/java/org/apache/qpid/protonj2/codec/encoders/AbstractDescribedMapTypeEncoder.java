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
package org.apache.qpid.protonj2.codec.encoders;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;

/**
 * Base class used for all Described Type objects that are represented as a List
 *
 * @param <M> the map type that is being encoded.
 * @param <K> the key type used for the encoded map's keys.
 * @param <V> the value type used for the encoded map's values.
 */
public abstract class AbstractDescribedMapTypeEncoder<K, V, M> extends AbstractDescribedTypeEncoder<M> {

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
    public byte getMapEncoding(M value) {
        return EncodingCodes.MAP32;
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
    public abstract boolean hasMap(M value);

    /**
     * Gets the number of elements that will result when this type is encoded
     * into an AMQP Map type.
     *
     * @param value
     * 		the value which will be encoded as a map type.
     *
     * @return the number of elements that should comprise the encoded list.
     */
    public abstract int getMapSize(M value);

    /**
     * Performs the write of the Map entries to the given buffer, the caller
     * takes care of writing the Map preamble and tracking the final size of
     * the written elements of the Map.
     *
     * @param buffer
     *      the buffer where the type should be encoded to.
     * @param state
     *      the current encoder state.
     * @param value
     * 		the value which will be encoded as a map type.
     */
    public abstract void writeMapEntries(ProtonBuffer buffer, EncoderState state, M value);

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, M value) {
        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        state.getEncoder().writeUnsignedLong(buffer, state, getDescriptorCode().byteValue());

        if (hasMap(value)) {
            final int count = getMapSize(value);
            final byte encodingCode = getMapEncoding(value);

            buffer.writeByte(encodingCode);

            switch (encodingCode) {
                case EncodingCodes.MAP8:
                    writeSmallType(buffer, state, value, count);
                    break;
                case EncodingCodes.MAP32:
                    writeLargeType(buffer, state, value, count);
                    break;
            }
        } else {
            state.getEncoder().writeNull(buffer, state);
        }
    }

    private void writeSmallType(ProtonBuffer buffer, EncoderState state, M value, int elementCount) {
        final int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeByte((byte) 0);
        buffer.writeByte((byte) (elementCount * 2));

        writeMapEntries(buffer, state, value);

        // Move back and write the size
        final int writeSize = (buffer.getWriteIndex() - startIndex) - Byte.BYTES;

        buffer.setByte(startIndex, writeSize);
    }

    private void writeLargeType(ProtonBuffer buffer, EncoderState state, M value, int elementCount) {
        final int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(elementCount * 2);

        writeMapEntries(buffer, state, value);

        // Move back and write the size
        final int writeSize = (buffer.getWriteIndex() - startIndex) - Integer.BYTES;

        buffer.setInt(startIndex, writeSize);
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        final int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(values.length);

        buffer.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        state.getEncoder().writeUnsignedLong(buffer, state, getDescriptorCode());

        writeRawArray(buffer, state, values);

        // Move back and write the size
        final int writeSize = buffer.getWriteIndex() - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size too large: " + writeSize);
        }

        buffer.setInt(startIndex, writeSize);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        buffer.writeByte(EncodingCodes.MAP32);

        for (int i = 0; i < values.length; ++i) {
            final M map = (M) values[i];
            final int count = getMapSize(map);
            final int mapStartIndex = buffer.getWriteIndex();

            // Reserve space for the size and write the count of list elements.
            buffer.writeInt(0);
            buffer.writeInt(count * 2);

            writeMapEntries(buffer, state, map);

            // Move back and write the size
            final int writeSize = buffer.getWriteIndex() - mapStartIndex - Integer.BYTES;

            buffer.setInt(mapStartIndex, writeSize);
        }
    }
}
