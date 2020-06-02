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
package org.apache.qpid.proton4j.codec.encoders;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Decimal128;
import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.EncodeException;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ArrayTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BinaryTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BooleanTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ByteTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.CharacterTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal128TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal32TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal64TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.DoubleTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.FloatTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.IntegerTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ListTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.LongTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.MapTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.NullTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ShortTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.StringTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.SymbolTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.TimestampTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UUIDTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedByteTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedIntegerTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedLongTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedShortTypeEncoder;

/**
 * The default AMQP Encoder implementation.
 */
public final class ProtonEncoder implements Encoder {

    // The encoders for primitives are fixed and cannot be altered by users who want
    // to register custom encoders, these encoders are stateless so they can be safely
    // made static to reduce overhead of creating and destroying this type.
    private static final ArrayTypeEncoder arrayEncoder = new ArrayTypeEncoder();
    private static final BinaryTypeEncoder binaryEncoder = new BinaryTypeEncoder();
    private static final BooleanTypeEncoder booleanEncoder = new BooleanTypeEncoder();
    private static final ByteTypeEncoder byteEncoder = new ByteTypeEncoder();
    private static final CharacterTypeEncoder charEncoder = new CharacterTypeEncoder();
    private static final Decimal32TypeEncoder decimal32Encoder = new Decimal32TypeEncoder();
    private static final Decimal64TypeEncoder decimal64Encoder = new Decimal64TypeEncoder();
    private static final Decimal128TypeEncoder decimal128Encoder = new Decimal128TypeEncoder();
    private static final DoubleTypeEncoder doubleEncoder = new DoubleTypeEncoder();
    private static final FloatTypeEncoder floatEncoder = new FloatTypeEncoder();
    private static final IntegerTypeEncoder integerEncoder = new IntegerTypeEncoder();
    private static final ListTypeEncoder listEncoder = new ListTypeEncoder();
    private static final LongTypeEncoder longEncoder = new LongTypeEncoder();
    private static final MapTypeEncoder mapEncoder = new MapTypeEncoder();
    private static final NullTypeEncoder nullEncoder = new NullTypeEncoder();
    private static final ShortTypeEncoder shortEncoder = new ShortTypeEncoder();
    private static final StringTypeEncoder stringEncoder = new StringTypeEncoder();
    private static final SymbolTypeEncoder symbolEncoder = new SymbolTypeEncoder();
    private static final TimestampTypeEncoder timestampEncoder = new TimestampTypeEncoder();
    private static final UnknownDescribedTypeEncoder unknownTypeEncoder = new UnknownDescribedTypeEncoder();
    private static final UUIDTypeEncoder uuidEncoder = new UUIDTypeEncoder();
    private static final UnsignedByteTypeEncoder ubyteEncoder = new UnsignedByteTypeEncoder();
    private static final UnsignedShortTypeEncoder ushortEncoder = new UnsignedShortTypeEncoder();
    private static final UnsignedIntegerTypeEncoder uintEncoder = new UnsignedIntegerTypeEncoder();
    private static final UnsignedLongTypeEncoder ulongEncoder = new UnsignedLongTypeEncoder();
    private static final DeliveryTagEncoder deliveryTagEncoder = new DeliveryTagEncoder();

    private ProtonEncoderState singleThreadedState;

    private final Map<Class<?>, TypeEncoder<?>> typeEncoders = new HashMap<>();
    {
        typeEncoders.put(arrayEncoder.getTypeClass(), arrayEncoder);
        typeEncoders.put(binaryEncoder.getTypeClass(), binaryEncoder);
        typeEncoders.put(booleanEncoder.getTypeClass(), booleanEncoder);
        typeEncoders.put(byteEncoder.getTypeClass(), byteEncoder);
        typeEncoders.put(charEncoder.getTypeClass(), charEncoder);
        typeEncoders.put(decimal32Encoder.getTypeClass(), decimal32Encoder);
        typeEncoders.put(decimal64Encoder.getTypeClass(), decimal64Encoder);
        typeEncoders.put(decimal128Encoder.getTypeClass(), decimal128Encoder);
        typeEncoders.put(doubleEncoder.getTypeClass(), doubleEncoder);
        typeEncoders.put(floatEncoder.getTypeClass(), floatEncoder);
        typeEncoders.put(integerEncoder.getTypeClass(), integerEncoder);
        typeEncoders.put(listEncoder.getTypeClass(), listEncoder);
        typeEncoders.put(longEncoder.getTypeClass(), longEncoder);
        typeEncoders.put(mapEncoder.getTypeClass(), mapEncoder);
        typeEncoders.put(nullEncoder.getTypeClass(), nullEncoder);
        typeEncoders.put(shortEncoder.getTypeClass(), shortEncoder);
        typeEncoders.put(stringEncoder.getTypeClass(), stringEncoder);
        typeEncoders.put(symbolEncoder.getTypeClass(), symbolEncoder);
        typeEncoders.put(timestampEncoder.getTypeClass(), timestampEncoder);
        typeEncoders.put(unknownTypeEncoder.getTypeClass(), unknownTypeEncoder);
        typeEncoders.put(uuidEncoder.getTypeClass(), uuidEncoder);
        typeEncoders.put(ubyteEncoder.getTypeClass(), ubyteEncoder);
        typeEncoders.put(ushortEncoder.getTypeClass(), ushortEncoder);
        typeEncoders.put(uintEncoder.getTypeClass(), uintEncoder);
        typeEncoders.put(ulongEncoder.getTypeClass(), ulongEncoder);
        typeEncoders.put(deliveryTagEncoder.getTypeClass(), deliveryTagEncoder);
    }

    @Override
    public ProtonEncoderState newEncoderState() {
        return new ProtonEncoderState(this);
    }

    @Override
    public ProtonEncoderState getCachedEncoderState() {
        ProtonEncoderState state = singleThreadedState;
        if (state == null) {
            state = newEncoderState();
        }

        return state.reset();
    }

    @Override
    public void writeNull(ProtonBuffer buffer, EncoderState state) throws EncodeException {
        nullEncoder.writeType(buffer, state, null);
    }

    @Override
    public void writeBoolean(ProtonBuffer buffer, EncoderState state, boolean value) throws EncodeException {
        booleanEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeBoolean(ProtonBuffer buffer, EncoderState state, Boolean value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            buffer.writeByte(value ? EncodingCodes.BOOLEAN_TRUE : EncodingCodes.BOOLEAN_FALSE);
        }
    }

    @Override
    public void writeUnsignedByte(ProtonBuffer buffer, EncoderState state, UnsignedByte value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            ubyteEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedByte(ProtonBuffer buffer, EncoderState state, byte value) throws EncodeException {
        ubyteEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, UnsignedShort value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            ushortEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, short value) throws EncodeException {
        ushortEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeUnsignedShort(ProtonBuffer buffer, EncoderState state, int value) throws EncodeException {
        if (value < 0) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            ushortEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, UnsignedInteger value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            uintEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, byte value) throws EncodeException {
        uintEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, int value) throws EncodeException {
        uintEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeUnsignedInteger(ProtonBuffer buffer, EncoderState state, long value) throws EncodeException {
        if (value < 0) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            uintEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, byte value) throws EncodeException {
        ulongEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, long value) throws EncodeException {
        ulongEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeUnsignedLong(ProtonBuffer buffer, EncoderState state, UnsignedLong value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            ulongEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeByte(ProtonBuffer buffer, EncoderState state, byte value) throws EncodeException {
        byteEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeByte(ProtonBuffer buffer, EncoderState state, Byte value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            byteEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeShort(ProtonBuffer buffer, EncoderState state, short value) throws EncodeException {
        shortEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeShort(ProtonBuffer buffer, EncoderState state, Short value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            shortEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeInteger(ProtonBuffer buffer, EncoderState state, int value) throws EncodeException {
        integerEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeInteger(ProtonBuffer buffer, EncoderState state, Integer value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            integerEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeLong(ProtonBuffer buffer, EncoderState state, long value) throws EncodeException {
        longEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeLong(ProtonBuffer buffer, EncoderState state, Long value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            longEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeFloat(ProtonBuffer buffer, EncoderState state, float value) throws EncodeException {
        floatEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeFloat(ProtonBuffer buffer, EncoderState state, Float value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            floatEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDouble(ProtonBuffer buffer, EncoderState state, double value) throws EncodeException {
        doubleEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeDouble(ProtonBuffer buffer, EncoderState state, Double value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            doubleEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDecimal32(ProtonBuffer buffer, EncoderState state, Decimal32 value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            decimal32Encoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDecimal64(ProtonBuffer buffer, EncoderState state, Decimal64 value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            decimal64Encoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDecimal128(ProtonBuffer buffer, EncoderState state, Decimal128 value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            decimal128Encoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeCharacter(ProtonBuffer buffer, EncoderState state, char value) throws EncodeException {
        charEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeCharacter(ProtonBuffer buffer, EncoderState state, Character value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            charEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeTimestamp(ProtonBuffer buffer, EncoderState state, long value) throws EncodeException {
        timestampEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeTimestamp(ProtonBuffer buffer, EncoderState state, Date value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            timestampEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUUID(ProtonBuffer buffer, EncoderState state, UUID value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            uuidEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeBinary(ProtonBuffer buffer, EncoderState state, Binary value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            binaryEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeBinary(ProtonBuffer buffer, EncoderState state, ProtonBuffer value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            binaryEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeBinary(ProtonBuffer buffer, EncoderState state, byte[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            binaryEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDeliveryTag(ProtonBuffer buffer, EncoderState state, DeliveryTag value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            deliveryTagEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeString(ProtonBuffer buffer, EncoderState state, String value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            stringEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeSymbol(ProtonBuffer buffer, EncoderState state, Symbol value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            symbolEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeSymbol(ProtonBuffer buffer, EncoderState state, String value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            symbolEncoder.writeType(buffer, state, Symbol.valueOf(value));
        }
    }

    @Override
    public <T> void writeList(ProtonBuffer buffer, EncoderState state, List<T> value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            listEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public <K, V> void writeMap(ProtonBuffer buffer, EncoderState state, Map<K, V> value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            mapEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDescribedType(ProtonBuffer buffer, EncoderState state, DescribedType value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            unknownTypeEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, boolean[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, byte[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, short[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, int[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, long[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, float[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, double[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, char[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Object[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Decimal32[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Decimal64[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Decimal128[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, Symbol[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedByte[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedShort[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedInteger[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, UnsignedLong[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ProtonBuffer buffer, EncoderState state, UUID[] value) throws EncodeException {
        if (value == null) {
            buffer.writeByte(EncodingCodes.NULL);
        } else {
            arrayEncoder.writeType(buffer, state, value);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void writeObject(ProtonBuffer buffer, EncoderState state, Object value) throws EncodeException {
        if (value != null) {
            TypeEncoder encoder = typeEncoders.get(value.getClass());

            if (encoder == null) {
                writeUnregisteredType(buffer, state, value);
            } else {
                encoder.writeType(buffer, state, value);
            }
        } else {
            buffer.writeByte(EncodingCodes.NULL);
        }
    }

    @SuppressWarnings("unchecked")
    private void writeUnregisteredType(ProtonBuffer buffer, EncoderState state, Object value) {
        if (value.getClass().isArray()) {
            Class<?> componentType = value.getClass().getComponentType();
            if (componentType.isPrimitive()) {
                if (componentType == Boolean.TYPE) {
                    writeArray(buffer, state, (boolean[]) value);
                } else if (componentType == Byte.TYPE) {
                    writeArray(buffer, state, (byte[]) value);
                } else if (componentType == Short.TYPE) {
                    writeArray(buffer, state, (short[]) value);
                } else if (componentType == Integer.TYPE) {
                    writeArray(buffer, state, (int[]) value);
                } else if (componentType == Long.TYPE) {
                    writeArray(buffer, state, (long[]) value);
                } else if (componentType == Float.TYPE) {
                    writeArray(buffer, state, (float[]) value);
                } else if (componentType == Double.TYPE) {
                    writeArray(buffer, state, (double[]) value);
                } else if (componentType == Character.TYPE) {
                    writeArray(buffer, state, (char[]) value);
                } else {
                    throw new IllegalArgumentException(
                        "Cannot write arrays of type " + componentType.getName());
                }
            } else {
                writeArray(buffer, state, (Object[]) value);
            }
        } else if (value instanceof List) {
            writeList(buffer, state, (List<Object>) value);
        } else if (value instanceof Map) {
            writeMap(buffer, state, (Map<Object, Object>) value);
        } else if (value instanceof DescribedType) {
            writeDescribedType(buffer, state, (DescribedType) value);
        } else {
            throw new IllegalArgumentException(
                "Do not know how to write Objects of class " + value.getClass().getName());
        }
    }

    @Override
    public <V> ProtonEncoder registerDescribedTypeEncoder(DescribedTypeEncoder<V> encoder) {
        typeEncoders.put(encoder.getTypeClass(), encoder);
        return this;
    }

    @Override
    public TypeEncoder<?> getTypeEncoder(Object value) {
        if (value == null) {
            return nullEncoder;
        } else {
            return getTypeEncoder(value.getClass(), value);
        }
    }

    @Override
    public TypeEncoder<?> getTypeEncoder(Class<?> typeClass) {
        return getTypeEncoder(typeClass, null);
    }

    public TypeEncoder<?> getTypeEncoder(Class<?> typeClass, Object instance) {
        TypeEncoder<?> encoder = typeEncoders.get(typeClass);

        if (encoder == null) {
            encoder = deduceTypeEncoder(typeClass, instance);
        }

        return encoder;
    }

    private TypeEncoder<?> deduceTypeEncoder(Class<?> typeClass, Object instance) {
        TypeEncoder<?> encoder = typeEncoders.get(typeClass);

        if (typeClass.isArray()) {
            encoder = arrayEncoder;
        } else {
            if (List.class.isAssignableFrom(typeClass)) {
                encoder = listEncoder;
            } else if (Map.class.isAssignableFrom(typeClass)) {
                encoder = mapEncoder;
            } else if (DescribedType.class.isAssignableFrom(typeClass)) {
                // For instances of a specific DescribedType that we don't know about the
                // generic described type encoder will work.  We don't use that though for
                // class lookups as we don't want to allow arrays of polymorphic types.
                if (encoder == null && instance != null) {
                    if (encoder == null) {
                        return unknownTypeEncoder;
                    }
                }
            }
        }

        // Ensure that next time we find the encoder immediately and don't need to
        // go through this process again.
        typeEncoders.put(typeClass, encoder);

        return encoder;
    }
}
