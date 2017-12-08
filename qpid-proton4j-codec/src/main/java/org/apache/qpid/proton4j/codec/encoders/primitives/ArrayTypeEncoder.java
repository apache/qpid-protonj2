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
package org.apache.qpid.proton4j.codec.encoders.primitives;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveArrayTypeEncoder;
import org.apache.qpid.proton4j.codec.TypeEncoder;

/**
 * Encoder of AMQP Array types to a byte stream.
 */
public class ArrayTypeEncoder implements PrimitiveArrayTypeEncoder {

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
    public void writeArrayElements(ProtonBuffer buffer, EncoderState state, Object[] value) {
        throw new IllegalArgumentException(
            "Do not know how to write Objects of class " + value.getClass().getName());

        // TODO - Need to figure out how to check that for Object[] arrays there is only
        //        one type of object in the array.  This could be just the encoder for the
        //        first element in the array then applied to the full array which should
        //        throw an error if the array contains mixed types.
    }

    //---- One Dimensional Optimized Array of Primitive Write methods --------//

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void writeArray(ProtonBuffer buffer, EncoderState state, Object[] value) {

        TypeEncoder typeEncoder = state.getEncoder().getTypeEncoder(value.getClass().getComponentType());
        if (typeEncoder == null) {
            throw new IllegalArgumentException(
                "Do not know how to write Objects of class " + value.getClass().getName());
        }

        // TODO - Need to figure out how to check that for Object[] arrays there is only
        //        one type of object in the array.  This could be just the encoder for the
        //        first element in the array then applied to the full array which should
        //        throw an error if the array contains mixed types.

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    //----- Write methods for primitive arrays -------------------------------//

    public void writeType(ProtonBuffer buffer, EncoderState state, boolean[] value) {
        final BooleanTypeEncoder typeEncoder = (BooleanTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, byte[] value) {
        final ByteTypeEncoder typeEncoder = (ByteTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, short[] value) {
        final ShortTypeEncoder typeEncoder = (ShortTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, int[] value) {
        final IntegerTypeEncoder typeEncoder = (IntegerTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, long[] value) {
        final LongTypeEncoder typeEncoder = (LongTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, float[] value) {
        final FloatTypeEncoder typeEncoder = (FloatTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, double[] value) {
        final DoubleTypeEncoder typeEncoder = (DoubleTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }

    public void writeType(ProtonBuffer buffer, EncoderState state, char[] value) {
        final CharacterTypeEncoder typeEncoder = (CharacterTypeEncoder) state.getEncoder().getTypeEncoder(Boolean.class);

        // Write the Array Type encoding code, we don't optimize here.
        buffer.writeByte(EncodingCodes.ARRAY32);

        int startIndex = buffer.getWriteIndex();

        // Reserve space for the size and write the count of list elements.
        buffer.writeInt(0);
        buffer.writeInt(value.length);

        // Write the array elements after writing the array length
        typeEncoder.writeArrayElements(buffer, state, value);

        // Move back and write the size
        int endIndex = buffer.getWriteIndex();
        long writeSize = endIndex - startIndex - Integer.BYTES;

        if (writeSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot encode given array, encoded size to large: " + writeSize);
        }

        buffer.setInt(startIndex, (int) writeSize);
    }
}
