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

import io.netty.buffer.ByteBuf;

/**
 * Base class used for all Described Type objects that are represented as a List
 *
 * @param <V> the type that is being encoded
 */
public interface DescribedListTypeEncoder<V> extends DescribedTypeEncoder<V> {

    @Override
    default void writeValue(ByteBuf buffer, EncoderState state, V value) {
        int count = getElementCount(value);
        int encodingCode = getLargestEncoding();

        // Optimized step, no other data to be written.
        if (count == 0 || encodingCode == EncodingCodes.LIST0) {
            buffer.writeByte(EncodingCodes.LIST0);
            return;
        }

        final int fieldWidth;

        if (encodingCode == EncodingCodes.LIST8) {
            fieldWidth = 1;
            buffer.writeByte(EncodingCodes.LIST8);
        } else {
            // TODO - Compute size needed to store the list and encode and then
            //        choose the width needed for write size and element count
            fieldWidth = 4;
            buffer.writeByte(EncodingCodes.LIST32);
        }

        int startIndex = buffer.writerIndex();

        // Reserve space for the size and write the count of list elements.
        if (fieldWidth == 1) {
            buffer.writeByte(0);
            buffer.writeByte(count);
        } else {
            buffer.writeInt(0);
            buffer.writeInt(count);
        }

        // Write the list elements and then compute total size written.
        for (int i = 0; i < count; ++i) {
            writeElement(value, i, buffer, state);
        }

        // Move back and write the size
        int endIndex = buffer.writerIndex();
        int writeSize = endIndex - startIndex - fieldWidth;

        if (fieldWidth == 1) {
            buffer.setByte(startIndex, writeSize);
        } else {
            buffer.setInt(startIndex, writeSize);
        }
    }

    /**
     * Returns the encoding code of the largest encoding this list type can be
     * encoded to.
     * <p>
     * Most encoders will return LIST32 but for cases where the type is know to
     * be encoded to LIST8 or always encodes an empty list (LIST0) the encoder can
     * optimize the encode step and not compute sizes.
     *
     * @return the encoding code of the largest list type needed for this object.
     */
    default int getLargestEncoding() {
        return EncodingCodes.LIST32 & 0xff;
    }

    /**
     * Instructs the encoder to write the element identified with the given index
     *
     * @param source
     *      the source of the list elements to write
     * @param index
     *      the element index that needs to be written
     * @param buffer
     *      the buffer to write the element to
     * @param state
     *      the current EncoderState value to use.
     */
    void writeElement(V source, int index, ByteBuf buffer, EncoderState state);

    /**
     * Gets the number of elements that will result when this type is encoded
     * into an AMQP List type.
     *
     * @param value
     * 		the value which will be encoded as a list type.
     *
     * @return the number of elements that should comprise the encoded list.
     */
    int getElementCount(V value);

}
