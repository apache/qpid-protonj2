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
package org.apache.qpid.proton4j.codec.decoders.primitives;

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.PrimitiveArrayTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.PrimitiveTypeDecoder;

/**
 * Base for the decoders of AMQP Array types.
 */
public abstract class AbstractArrayTypeDecoder extends AbstractPrimitiveTypeDecoder<Object[]> implements PrimitiveArrayTypeDecoder {

    @Override
    public Class<Object[]> getTypeClass() {
        return Object[].class;
    }

    @Override
    public boolean isArrayType() {
        return true;
    }

    @Override
    public Object[] readValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        return readValueAsObjectArray(buffer, state);
    }

    @Override
    public Object[] readValueAsObjectArray(ProtonBuffer buffer, DecoderState state) throws IOException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        if (getTypeCode() == (EncodingCodes.ARRAY32 & 0xff)) {
            size -= 8; // 4 bytes each for size and count;
        } else {
            size -= 2; // 1 byte each for size and count;
        }

        if (size > buffer.getReadableBytes()) {
            throw new IllegalArgumentException(String.format(
                "Array size indicated %d is greater than the amount of data available to decode (%d)",
                size, buffer.getReadableBytes()));
        }

        return decodeAsArray(buffer, state, count);
    }

    @Override
    public Object readValueAsObject(ProtonBuffer buffer, DecoderState state) throws IOException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        if (getTypeCode() == (EncodingCodes.ARRAY32 & 0xff)) {
            size -= 8; // 4 bytes each for size and count;
        } else {
            size -= 2; // 1 byte each for size and count;
        }

        if (size > buffer.getReadableBytes()) {
            throw new IllegalArgumentException(String.format(
                "Array size indicated %d is greater than the amount of data available to decode (%d)",
                size, buffer.getReadableBytes()));
        }

        return decodeAsObject(buffer, state, count);
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws IOException {
        buffer.skipBytes(readSize(buffer));
    }

    protected abstract int readSize(ProtonBuffer buffer);

    protected abstract int readCount(ProtonBuffer buffer);

    private static Object[] decodeAsArray(ProtonBuffer buffer, DecoderState state, final int count) throws IOException {
        PrimitiveTypeDecoder<?> decoder = (PrimitiveTypeDecoder<?>) state.getDecoder().readNextTypeDecoder(buffer, state);
        return decodeNonPrimitiveArray(decoder, buffer, state, count);
    }

    private static Object[] decodeNonPrimitiveArray(TypeDecoder<?> decoder, ProtonBuffer buffer, DecoderState state, int count) throws IOException {

        if (count > buffer.getReadableBytes()) {
            throw new IllegalArgumentException(String.format(
                "Array element count %d is specified to be greater than the amount of data available (%d)",
                count, buffer.getReadableBytes()));
        }

        if (decoder.isArrayType()) {
            PrimitiveArrayTypeDecoder arrayDecoder = (PrimitiveArrayTypeDecoder) decoder;

            Object[] array = new Object[count];
            for (int i = 0; i < count; i++) {
                array[i] = arrayDecoder.readValueAsObject(buffer, state);
            }

            return array;
        } else {
            return decoder.readArrayElements(buffer, state, count);
        }
    }

    private static Object decodeAsObject(ProtonBuffer buffer, DecoderState state, int count) throws IOException {

        TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof PrimitiveTypeDecoder) {
            PrimitiveTypeDecoder<?> primitiveTypeDecoder = (PrimitiveTypeDecoder<?>) decoder;
            if (primitiveTypeDecoder.isJavaPrimitive()) {
                if (count > buffer.getReadableBytes()) {
                    throw new IllegalArgumentException(String.format(
                        "Array element count %d is specified to be greater than the amount of data available (%d)",
                        count, buffer.getReadableBytes()));
                }

                Class<?> typeClass = decoder.getTypeClass();

                if (Boolean.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((BooleanTypeDecoder) decoder, buffer, state, count);
                } else if (Byte.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((ByteTypeDecoder) decoder, buffer, state, count);
                } else if (Short.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((ShortTypeDecoder) decoder, buffer, state, count);
                } else if (Integer.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((Integer32TypeDecoder) decoder, buffer, state, count);
                } else if (Long.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((LongTypeDecoder) decoder, buffer, state, count);
                } else if (Double.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((DoubleTypeDecoder) decoder, buffer, state, count);
                } else if (Float.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((FloatTypeDecoder) decoder, buffer, state, count);
                } else {
                    throw new ClassCastException("Unexpected class " + decoder.getClass().getName());
                }
            }
        }

        return decodeNonPrimitiveArray(decoder, buffer, state, count);
    }

    private static boolean[] decodePrimitiveTypeArray(BooleanTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        boolean[] array = new boolean[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static byte[] decodePrimitiveTypeArray(ByteTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        byte[] array = new byte[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static short[] decodePrimitiveTypeArray(ShortTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        short[] array = new short[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static int[] decodePrimitiveTypeArray(Integer32TypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        int[] array = new int[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static long[] decodePrimitiveTypeArray(LongTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        long[] array = new long[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static float[] decodePrimitiveTypeArray(FloatTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        float[] array = new float[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static double[] decodePrimitiveTypeArray(DoubleTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        double[] array = new double[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }
}
