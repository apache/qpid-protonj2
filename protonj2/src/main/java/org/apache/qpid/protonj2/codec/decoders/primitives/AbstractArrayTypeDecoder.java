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
package org.apache.qpid.protonj2.codec.decoders.primitives;

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.AbstractPrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveArrayTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.PrimitiveTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.ProtonStreamUtils;

/**
 * Base for the decoders of AMQP Array types that defaults to returning opaque Object
 * values to match what the other decoders do.  External decoding tools will need to use
 * the {@link PrimitiveArrayTypeDecoder#isArrayType()} checks to determine how they want
 * to read and return array types.
 */
public abstract class AbstractArrayTypeDecoder extends AbstractPrimitiveTypeDecoder<Object> implements PrimitiveArrayTypeDecoder {

    @Override
    public Class<Object> getTypeClass() {
        return Object.class;
    }

    @Override
    public boolean isArrayType() {
        return true;
    }

    @Override
    public Object readValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        return readValueAsObject(buffer, state);
    }

    @Override
    public Object[] readValueAsObjectArray(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        if (getTypeCode() == (EncodingCodes.ARRAY32 & 0xff)) {
            size -= 8; // 4 bytes each for size and count;
        } else {
            size -= 2; // 1 byte each for size and count;
        }

        if (size > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                "Array size indicated %d is greater than the amount of data available to decode (%d)",
                size, buffer.getReadableBytes()));
        }

        return decodeAsArray(buffer, state, count);
    }

    @Override
    public Object readValueAsObject(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        int size = readSize(buffer);
        int count = readCount(buffer);

        if (getTypeCode() == (EncodingCodes.ARRAY32 & 0xff)) {
            size -= 8; // 4 bytes each for size and count;
        } else {
            size -= 2; // 1 byte each for size and count;
        }

        if (size > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                "Array size indicated %d is greater than the amount of data available to decode (%d)",
                size, buffer.getReadableBytes()));
        }

        return decodeAsObject(buffer, state, count);
    }

    @Override
    public Object readValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        return readValueAsObject(stream, state);
    }

    @Override
    public Object[] readValueAsObjectArray(InputStream stream, StreamDecoderState state) throws DecodeException {
        readSize(stream);

        return decodeAsArray(stream, state, readCount(stream));
    }

    @Override
    public Object readValueAsObject(InputStream stream, StreamDecoderState state) throws DecodeException {
        readSize(stream);

        return decodeAsObject(stream, state, readCount(stream));
    }

    @Override
    public void skipValue(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        buffer.skipBytes(readSize(buffer));
    }

    @Override
    public void skipValue(InputStream stream, StreamDecoderState state) throws DecodeException {
        ProtonStreamUtils.skipBytes(stream, readSize(stream));
    }

    protected abstract int readSize(ProtonBuffer buffer);

    protected abstract int readCount(ProtonBuffer buffer);

    protected abstract int readSize(InputStream stream);

    protected abstract int readCount(InputStream stream);

    private static Object[] decodeAsArray(ProtonBuffer buffer, DecoderState state, final int count) throws DecodeException {
        final PrimitiveTypeDecoder<?> decoder = (PrimitiveTypeDecoder<?>) state.getDecoder().readNextTypeDecoder(buffer, state);

        return decodeNonPrimitiveArray(decoder, buffer, state, count);
    }

    private static Object[] decodeNonPrimitiveArray(TypeDecoder<?> decoder, ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        if (count > buffer.getReadableBytes()) {
            throw new DecodeException(String.format(
                "Array element count %d is specified to be greater than the amount of data available (%d)",
                count, buffer.getReadableBytes()));
        }

        if (decoder.isArrayType()) {
            final PrimitiveArrayTypeDecoder arrayDecoder = (PrimitiveArrayTypeDecoder) decoder;

            final Object[] array = new Object[count];
            for (int i = 0; i < count; i++) {
                array[i] = arrayDecoder.readValueAsObject(buffer, state);
            }

            return array;
        } else {
            return decoder.readArrayElements(buffer, state, count);
        }
    }

    private static Object decodeAsObject(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        final TypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(buffer, state);

        if (decoder instanceof PrimitiveTypeDecoder) {
            final PrimitiveTypeDecoder<?> primitiveTypeDecoder = (PrimitiveTypeDecoder<?>) decoder;
            if (primitiveTypeDecoder.isJavaPrimitive()) {
                final int typeCode = ((PrimitiveTypeDecoder<?>) decoder).getTypeCode();

                if (typeCode != EncodingCodes.BOOLEAN_TRUE &&
                    typeCode != EncodingCodes.BOOLEAN_FALSE &&
                    typeCode != EncodingCodes.NULL) {

                    if (count > buffer.getReadableBytes()) {
                        throw new DecodeException(String.format(
                            "Array element count %d is specified to be greater than the amount of data available (%d)",
                            count, buffer.getReadableBytes()));
                    }
                }

                final Class<?> typeClass = decoder.getTypeClass();

                if (Boolean.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((BooleanTypeDecoder) decoder, buffer, state, count);
                } else if (Byte.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((ByteTypeDecoder) decoder, buffer, state, count);
                } else if (Short.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((ShortTypeDecoder) decoder, buffer, state, count);
                } else if (Integer.class.equals(typeClass)) {
                    if (primitiveTypeDecoder.getTypeCode() == (EncodingCodes.INT & 0xff)) {
                        return decodePrimitiveTypeArray((Integer32TypeDecoder) decoder, buffer, state, count);
                    } else {
                        return decodePrimitiveTypeArray((Integer8TypeDecoder) decoder, buffer, state, count);
                    }
                } else if (Long.class.equals(typeClass)) {
                    if (primitiveTypeDecoder.getTypeCode() == (EncodingCodes.LONG & 0xff)) {
                        return decodePrimitiveTypeArray((LongTypeDecoder) decoder, buffer, state, count);
                    } else {
                        return decodePrimitiveTypeArray((Long8TypeDecoder) decoder, buffer, state, count);
                    }
                } else if (Double.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((DoubleTypeDecoder) decoder, buffer, state, count);
                } else if (Float.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((FloatTypeDecoder) decoder, buffer, state, count);
                } else if (Character.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((CharacterTypeDecoder) decoder, buffer, state, count);
                } else {
                    throw new DecodeException("Unexpected class " + decoder.getClass().getName());
                }
            }
        }

        return decodeNonPrimitiveArray(decoder, buffer, state, count);
    }

    private static boolean[] decodePrimitiveTypeArray(BooleanTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final boolean[] array = new boolean[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static byte[] decodePrimitiveTypeArray(ByteTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final byte[] array = new byte[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static char[] decodePrimitiveTypeArray(CharacterTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final char[] array = new char[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static short[] decodePrimitiveTypeArray(ShortTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final short[] array = new short[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static int[] decodePrimitiveTypeArray(Integer32TypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final int[] array = new int[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static int[] decodePrimitiveTypeArray(Integer8TypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final int[] array = new int[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static long[] decodePrimitiveTypeArray(LongTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final long[] array = new long[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static long[] decodePrimitiveTypeArray(Long8TypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final long[] array = new long[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static float[] decodePrimitiveTypeArray(FloatTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final float[] array = new float[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    private static double[] decodePrimitiveTypeArray(DoubleTypeDecoder decoder, ProtonBuffer buffer, DecoderState state, int count) {
        final double[] array = new double[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(buffer, state);
        }

        return array;
    }

    //----- InputStream based array decoding

    private static Object[] decodeAsArray(InputStream stream, StreamDecoderState state, final int count) throws DecodeException {
        final PrimitiveTypeDecoder<?> decoder = (PrimitiveTypeDecoder<?>) state.getDecoder().readNextTypeDecoder(stream, state);

        return decodeNonPrimitiveArray(decoder, stream, state, count);
    }

    private static Object[] decodeNonPrimitiveArray(StreamTypeDecoder<?> decoder, InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        if (decoder.isArrayType()) {
            final PrimitiveArrayTypeDecoder arrayDecoder = (PrimitiveArrayTypeDecoder) decoder;

            final Object[] array = new Object[count];
            for (int i = 0; i < count; i++) {
                array[i] = arrayDecoder.readValueAsObject(stream, state);
            }

            return array;
        } else {
            return decoder.readArrayElements(stream, state, count);
        }
    }

    private static Object decodeAsObject(InputStream stream, StreamDecoderState state, int count) throws DecodeException {
        final StreamTypeDecoder<?> decoder = state.getDecoder().readNextTypeDecoder(stream, state);

        if (decoder instanceof PrimitiveTypeDecoder) {
            final PrimitiveTypeDecoder<?> primitiveTypeDecoder = (PrimitiveTypeDecoder<?>) decoder;
            if (primitiveTypeDecoder.isJavaPrimitive()) {
                final Class<?> typeClass = decoder.getTypeClass();

                if (Boolean.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((BooleanTypeDecoder) decoder, stream, state, count);
                } else if (Byte.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((ByteTypeDecoder) decoder, stream, state, count);
                } else if (Short.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((ShortTypeDecoder) decoder, stream, state, count);
                } else if (Integer.class.equals(typeClass)) {
                    if (primitiveTypeDecoder.getTypeCode() == (EncodingCodes.INT & 0xff)) {
                        return decodePrimitiveTypeArray((Integer32TypeDecoder) decoder, stream, state, count);
                    } else {
                        return decodePrimitiveTypeArray((Integer8TypeDecoder) decoder, stream, state, count);
                    }
                } else if (Long.class.equals(typeClass)) {
                    if (primitiveTypeDecoder.getTypeCode() == (EncodingCodes.LONG & 0xff)) {
                        return decodePrimitiveTypeArray((LongTypeDecoder) decoder, stream, state, count);
                    } else {
                        return decodePrimitiveTypeArray((Long8TypeDecoder) decoder, stream, state, count);
                    }
                } else if (Double.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((DoubleTypeDecoder) decoder, stream, state, count);
                } else if (Float.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((FloatTypeDecoder) decoder, stream, state, count);
                } else if (Character.class.equals(typeClass)) {
                    return decodePrimitiveTypeArray((CharacterTypeDecoder) decoder, stream, state, count);
                } else {
                    throw new DecodeException("Unexpected class " + decoder.getClass().getName());
                }
            }
        }

        return decodeNonPrimitiveArray(decoder, stream, state, count);
    }

    private static boolean[] decodePrimitiveTypeArray(BooleanTypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final boolean[] array = new boolean[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static byte[] decodePrimitiveTypeArray(ByteTypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final byte[] array = new byte[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static char[] decodePrimitiveTypeArray(CharacterTypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final char[] array = new char[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static short[] decodePrimitiveTypeArray(ShortTypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final short[] array = new short[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static int[] decodePrimitiveTypeArray(Integer32TypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final int[] array = new int[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static int[] decodePrimitiveTypeArray(Integer8TypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final int[] array = new int[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static long[] decodePrimitiveTypeArray(LongTypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final long[] array = new long[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static long[] decodePrimitiveTypeArray(Long8TypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final long[] array = new long[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static float[] decodePrimitiveTypeArray(FloatTypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final float[] array = new float[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }

    private static double[] decodePrimitiveTypeArray(DoubleTypeDecoder decoder, InputStream stream, StreamDecoderState state, int count) {
        final double[] array = new double[count];

        for (int i = 0; i < count; i++) {
            array[i] = decoder.readPrimitiveValue(stream, state);
        }

        return array;
    }
}
