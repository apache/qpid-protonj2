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
package org.apache.qpid.protonj2.codec.encoders.primitives;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.EncoderState;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeEncoder;
import org.apache.qpid.protonj2.codec.encoders.PrimitiveTypeEncoder;

/**
 * Encoder of AMQP Array types to a byte stream.
 */
public final class ArrayTypeEncoder implements PrimitiveTypeEncoder<Object> {

    @Override
    public boolean isArrayType() {
        return true;
    }

    @Override
    public Class<Object> getTypeClass() {
        return Object.class;
    }

    @Override
    public void writeType(ProtonBuffer buffer, EncoderState state, Object value) {
        if (!value.getClass().isArray()) {
            throw new IllegalArgumentException("Expected Array type but got: " + value.getClass().getSimpleName());
        }

        Class<?> componentType = value.getClass().getComponentType();
        if (componentType.isPrimitive()) {
            if (componentType == Boolean.TYPE) {
                writeType(buffer, state, (boolean[]) value);
            } else if (componentType == Byte.TYPE) {
                writeType(buffer, state, (byte[]) value);
            } else if (componentType == Short.TYPE) {
                writeType(buffer, state, (short[]) value);
            } else if (componentType == Integer.TYPE) {
                writeType(buffer, state, (int[]) value);
            } else if (componentType == Long.TYPE) {
                writeType(buffer, state, (long[]) value);
            } else if (componentType == Float.TYPE) {
                writeType(buffer, state, (float[]) value);
            } else if (componentType == Double.TYPE) {
                writeType(buffer, state, (double[]) value);
            } else if (componentType == Character.TYPE) {
                writeType(buffer, state, (char[]) value);
            } else {
                throw new IllegalArgumentException(
                    "Cannot write arrays of type " + componentType.getName());
            }
        } else {
            writeArray(buffer, state, (Object[]) value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        TypeEncoder<?> typeEncoder = findTypeEncoder(buffer, state, values);

        // If the is an array of arrays then we need to control the encoding code
        // and size indicators and hand off writing the entries to the raw write
        // method which will follow the nested path.
        if (typeEncoder.isArrayType()) {
            // Write the Array Type encoding code, we don't optimize here.
            buffer.writeByte(EncodingCodes.ARRAY32);

            final int startIndex = buffer.getWriteOffset();

            // Reserve space for the size and write the count of list elements.
            buffer.writeInt(0);
            buffer.writeInt(values.length);

            // Write the arrays as a raw series of arrays accounting for nested arrays
            writeRawArray(buffer, state, values);

            // Move back and write the size
            final long writeSize = buffer.getWriteOffset() - startIndex - Integer.BYTES;

            if (writeSize > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
            }

            buffer.setInt(startIndex, (int) writeSize);
        } else {
            typeEncoder.writeArray(buffer, state, values);
        }
    }

    @Override
    public void writeRawArray(ProtonBuffer buffer, EncoderState state, Object[] values) {
        TypeEncoder<?> typeEncoder = findTypeEncoder(buffer, state, values[0]);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        for (int i = 0; i < values.length; ++i) {
            final int startIndex = buffer.getWriteOffset();

            // Reserve space for the size and write the count of list elements.
            buffer.writeInt(0);

            typeEncoder = findTypeEncoder(buffer, state, values[i]);
            if (values[i].getClass().getComponentType().isPrimitive()) {
                Class<?> componentType = values[i].getClass().getComponentType();

                if (componentType == Boolean.TYPE) {
                    buffer.writeInt(((boolean[]) values[i]).length);
                    ((BooleanTypeEncoder) typeEncoder).writeRawArray(buffer, state, (boolean[]) values[i]);
                } else if (componentType == Byte.TYPE) {
                    buffer.writeInt(((byte[]) values[i]).length);
                    ((ByteTypeEncoder) typeEncoder).writeRawArray(buffer, state, (byte[]) values[i]);
                } else if (componentType == Short.TYPE) {
                    buffer.writeInt(((short[]) values[i]).length);
                    ((ShortTypeEncoder) typeEncoder).writeRawArray(buffer, state, (short[]) values[i]);
                } else if (componentType == Integer.TYPE) {
                    buffer.writeInt(((int[]) values[i]).length);
                    ((IntegerTypeEncoder) typeEncoder).writeRawArray(buffer, state, (int[]) values[i]);
                } else if (componentType == Long.TYPE) {
                    buffer.writeInt(((long[]) values[i]).length);
                    ((LongTypeEncoder) typeEncoder).writeRawArray(buffer, state, (long[]) values[i]);
                } else if (componentType == Float.TYPE) {
                    buffer.writeInt(((Object[]) values[i]).length);
                    ((FloatTypeEncoder) typeEncoder).writeRawArray(buffer, state, (float[]) values[i]);
                } else if (componentType == Double.TYPE) {
                    buffer.writeInt(((double[]) values[i]).length);
                    ((DoubleTypeEncoder) typeEncoder).writeRawArray(buffer, state, (double[]) values[i]);
                } else if (componentType == Character.TYPE) {
                    buffer.writeInt(((float[]) values[i]).length);
                    ((CharacterTypeEncoder) typeEncoder).writeRawArray(buffer, state, (char[]) values[i]);
                } else {
                    throw new IllegalArgumentException(
                        "Cannot write arrays of type " + componentType.getName());
                }
            } else {
                buffer.writeInt(((Object[]) values[i]).length);
                typeEncoder.writeRawArray(buffer, state, (Object[]) values[i]);
            }

            // Move back and write the size
            final long writeSize = buffer.getWriteOffset() - startIndex - Integer.BYTES;

            if (writeSize > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
            }

            buffer.setInt(startIndex, (int) writeSize);
        }
    }

    //----- Write methods for primitive arrays -------------------------------//

    /**
     * Encodes the given array into the provided buffer for transmission.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the array encoding should be written.
     * @param state
     *      The {@link EncoderState} which can be used when encoding the array elements.
     * @param value
     *      The array that should be encoded using this {@link ArrayTypeEncoder}.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, boolean[] value) {
        final BooleanTypeEncoder typeEncoder = (BooleanTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);
        typeEncoder.writeArray(buffer, state, value);
    }

    /**
     * Encodes the given array into the provided buffer for transmission.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the array encoding should be written.
     * @param state
     *      The {@link EncoderState} which can be used when encoding the array elements.
     * @param value
     *      The array that should be encoded using this {@link ArrayTypeEncoder}.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, byte[] value) {
        final ByteTypeEncoder typeEncoder = (ByteTypeEncoder) state.getEncoder().getTypeEncoder(Byte.class);
        typeEncoder.writeArray(buffer, state, value);
    }

    /**
     * Encodes the given array into the provided buffer for transmission.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the array encoding should be written.
     * @param state
     *      The {@link EncoderState} which can be used when encoding the array elements.
     * @param value
     *      The array that should be encoded using this {@link ArrayTypeEncoder}.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, short[] value) {
        final ShortTypeEncoder typeEncoder = (ShortTypeEncoder) state.getEncoder().getTypeEncoder(Short.class);
        typeEncoder.writeArray(buffer, state, value);
    }

    /**
     * Encodes the given array into the provided buffer for transmission.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the array encoding should be written.
     * @param state
     *      The {@link EncoderState} which can be used when encoding the array elements.
     * @param value
     *      The array that should be encoded using this {@link ArrayTypeEncoder}.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, int[] value) {
        final IntegerTypeEncoder typeEncoder = (IntegerTypeEncoder) state.getEncoder().getTypeEncoder(Integer.class);
        typeEncoder.writeArray(buffer, state, value);
    }

    /**
     * Encodes the given array into the provided buffer for transmission.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the array encoding should be written.
     * @param state
     *      The {@link EncoderState} which can be used when encoding the array elements.
     * @param value
     *      The array that should be encoded using this {@link ArrayTypeEncoder}.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, long[] value) {
        final LongTypeEncoder typeEncoder = (LongTypeEncoder) state.getEncoder().getTypeEncoder(Long.class);
        typeEncoder.writeArray(buffer, state, value);
    }

    /**
     * Encodes the given array into the provided buffer for transmission.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the array encoding should be written.
     * @param state
     *      The {@link EncoderState} which can be used when encoding the array elements.
     * @param value
     *      The array that should be encoded using this {@link ArrayTypeEncoder}.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, float[] value) {
        final FloatTypeEncoder typeEncoder = (FloatTypeEncoder) state.getEncoder().getTypeEncoder(Float.class);
        typeEncoder.writeArray(buffer, state, value);
    }

    /**
     * Encodes the given array into the provided buffer for transmission.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the array encoding should be written.
     * @param state
     *      The {@link EncoderState} which can be used when encoding the array elements.
     * @param value
     *      The array that should be encoded using this {@link ArrayTypeEncoder}.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, double[] value) {
        final DoubleTypeEncoder typeEncoder = (DoubleTypeEncoder) state.getEncoder().getTypeEncoder(Double.class);
        typeEncoder.writeArray(buffer, state, value);
    }

    /**
     * Encodes the given array into the provided buffer for transmission.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} where the array encoding should be written.
     * @param state
     *      The {@link EncoderState} which can be used when encoding the array elements.
     * @param value
     *      The array that should be encoded using this {@link ArrayTypeEncoder}.
     */
    public void writeType(ProtonBuffer buffer, EncoderState state, char[] value) {
        final CharacterTypeEncoder typeEncoder = (CharacterTypeEncoder) state.getEncoder().getTypeEncoder(Character.class);
        typeEncoder.writeArray(buffer, state, value);
    }

    //----- Internal support methods -----------------------------------------//

    private TypeEncoder<?> findTypeEncoder(ProtonBuffer buffer, EncoderState state, Object value) {
        // Scan the array until we either determine an appropriate TypeEncoder or find
        // that the array is such that encoding it would be invalid.

        if (!value.getClass().isArray()) {
            throw new IllegalArgumentException("Expected Array type but got: " + value.getClass().getSimpleName());
        }

        TypeEncoder<?> typeEncoder = null;

        if (value.getClass().getComponentType().isPrimitive()) {
            Class<?> componentType = value.getClass().getComponentType();

            if (componentType == Boolean.TYPE) {
                typeEncoder = state.getEncoder().getTypeEncoder(Boolean.class);
            } else if (componentType == Byte.TYPE) {
                typeEncoder = state.getEncoder().getTypeEncoder(Byte.class);
            } else if (componentType == Short.TYPE) {
                typeEncoder = state.getEncoder().getTypeEncoder(Short.class);
            } else if (componentType == Integer.TYPE) {
                typeEncoder = state.getEncoder().getTypeEncoder(Integer.class);
            } else if (componentType == Long.TYPE) {
                typeEncoder = state.getEncoder().getTypeEncoder(Long.class);
            } else if (componentType == Float.TYPE) {
                typeEncoder = state.getEncoder().getTypeEncoder(Float.class);
            } else if (componentType == Double.TYPE) {
                typeEncoder = state.getEncoder().getTypeEncoder(Double.class);
            } else if (componentType == Character.TYPE) {
                typeEncoder = state.getEncoder().getTypeEncoder(Character.class);
            } else {
                throw new IllegalArgumentException(
                    "Cannot write arrays of type " + componentType.getName());
            }
        } else {
            Object[] array = (Object[]) value;

            if (array.length == 0) {
                if (value.getClass().getComponentType().equals((Object.class))) {
                    throw new IllegalArgumentException(
                        "Cannot write a zero sized untyped array.");
                } else {
                    typeEncoder = state.getEncoder().getTypeEncoder(value.getClass().getComponentType());
                }
            } else {
                if (array[0].getClass().isArray()) {
                    typeEncoder = this;
                } else {
                    if (array[0].getClass().equals((Object.class))) {
                        throw new IllegalArgumentException(
                            "Cannot write a zero sized untyped array.");
                    }

                    typeEncoder = state.getEncoder().getTypeEncoder(array[0].getClass());
                }
            }
        }

        if (typeEncoder == null) {
            throw new IllegalArgumentException(
                "Do not know how to write Objects of class " + value.getClass().getName());
        }

        return typeEncoder;
    }
}
