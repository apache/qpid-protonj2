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
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.codec.Encoder;
import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ArrayTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BinaryTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BooleanFalseTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BooleanTrueTypeEncoder;
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

import io.netty.buffer.ByteBuf;

/**
 * The default AMQP Encoder implementation.
 */
public class ProtonEncoder implements Encoder {

    private final ArrayTypeEncoder arrayEncoder = new ArrayTypeEncoder();
    private final BinaryTypeEncoder binaryEncoder = new BinaryTypeEncoder();
    private final BooleanTypeEncoder booleanEncoder = new BooleanTypeEncoder();
    private final BooleanTrueTypeEncoder trueEncoder = new BooleanTrueTypeEncoder();
    private final BooleanFalseTypeEncoder falseEncoder = new BooleanFalseTypeEncoder();
    private final ByteTypeEncoder byteEncoder = new ByteTypeEncoder();
    private final CharacterTypeEncoder charEncoder = new CharacterTypeEncoder();
    private final Decimal32TypeEncoder decimal32Encoder = new Decimal32TypeEncoder();
    private final Decimal64TypeEncoder decimal64Encoder = new Decimal64TypeEncoder();
    private final Decimal128TypeEncoder decimal128Encoder = new Decimal128TypeEncoder();
    private final DoubleTypeEncoder doubleEncoder = new DoubleTypeEncoder();
    private final FloatTypeEncoder floatEncoder = new FloatTypeEncoder();
    private final IntegerTypeEncoder integerEncoder = new IntegerTypeEncoder();
    private final ListTypeEncoder listEncoder = new ListTypeEncoder();
    private final LongTypeEncoder longEncoder = new LongTypeEncoder();
    private final MapTypeEncoder mapEncoder = new MapTypeEncoder();
    private final NullTypeEncoder nullEncoder = new NullTypeEncoder();
    private final ShortTypeEncoder shortEncoder = new ShortTypeEncoder();
    private final StringTypeEncoder stringEncoder = new StringTypeEncoder();
    private final SymbolTypeEncoder symbolEncoder = new SymbolTypeEncoder();
    private final TimestampTypeEncoder timestampEncoder = new TimestampTypeEncoder();
    private final UnknownDescribedTypeEncoder unknownTypeEncoder = new UnknownDescribedTypeEncoder();
    private final UUIDTypeEncoder uuidEncoder = new UUIDTypeEncoder();
    private final UnsignedByteTypeEncoder ubyteEncoder = new UnsignedByteTypeEncoder();
    private final UnsignedShortTypeEncoder ushortEncoder = new UnsignedShortTypeEncoder();
    private final UnsignedIntegerTypeEncoder uintEncoder = new UnsignedIntegerTypeEncoder();
    private final UnsignedLongTypeEncoder ulongEncoder = new UnsignedLongTypeEncoder();

    private final Map<Class<?>, TypeEncoder<?>> typeEncoders = new HashMap<>();

    @Override
    public EncoderState newEncoderState() {
        return new ProtonEncoderState(this);
    }

    @Override
    public void writeNull(ByteBuf buffer, EncoderState state) {
        nullEncoder.writeType(buffer, state, null);
    }

    @Override
    public void writeBoolean(ByteBuf buffer, EncoderState state, boolean value) {
        if (value) {
            trueEncoder.writeType(buffer, state, value);
        } else {
            falseEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeBoolean(ByteBuf buffer, EncoderState state, Boolean value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            booleanEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedByte(ByteBuf buffer, EncoderState state, UnsignedByte value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            ubyteEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedShort(ByteBuf buffer, EncoderState state, UnsignedShort value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            ushortEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedInteger(ByteBuf buffer, EncoderState state, UnsignedInteger value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            uintEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUnsignedLong(ByteBuf buffer, EncoderState state, UnsignedLong value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            ulongEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeByte(ByteBuf buffer, EncoderState state, byte value) {
        byteEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeByte(ByteBuf buffer, EncoderState state, Byte value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            byteEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeShort(ByteBuf buffer, EncoderState state, short value) {
        shortEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeShort(ByteBuf buffer, EncoderState state, Short value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            shortEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeInteger(ByteBuf buffer, EncoderState state, int value) {
        integerEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeInteger(ByteBuf buffer, EncoderState state, Integer value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            integerEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeLong(ByteBuf buffer, EncoderState state, long value) {
        longEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeLong(ByteBuf buffer, EncoderState state, Long value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            longEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeFloat(ByteBuf buffer, EncoderState state, float value) {
        floatEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeFloat(ByteBuf buffer, EncoderState state, Float value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            floatEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDouble(ByteBuf buffer, EncoderState state, double value) {
        doubleEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeDouble(ByteBuf buffer, EncoderState state, Double value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            doubleEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDecimal32(ByteBuf buffer, EncoderState state, Decimal32 value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            decimal32Encoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDecimal64(ByteBuf buffer, EncoderState state, Decimal64 value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            decimal64Encoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDecimal128(ByteBuf buffer, EncoderState state, Decimal128 value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            decimal128Encoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeCharacter(ByteBuf buffer, EncoderState state, char value) {
        charEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeCharacter(ByteBuf buffer, EncoderState state, Character value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            charEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeTimestamp(ByteBuf buffer, EncoderState state, long value) {
        timestampEncoder.writeType(buffer, state, value);
    }

    @Override
    public void writeTimestamp(ByteBuf buffer, EncoderState state, Date value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            timestampEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeUUID(ByteBuf buffer, EncoderState state, UUID value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            uuidEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeBinary(ByteBuf buffer, EncoderState state, Binary value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            binaryEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeString(ByteBuf buffer, EncoderState state, String value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            stringEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeSymbol(ByteBuf buffer, EncoderState state, Symbol value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            symbolEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public <T> void writeList(ByteBuf buffer, EncoderState state, List<T> value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            listEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public <K, V> void writeMap(ByteBuf buffer, EncoderState state, Map<K, V> value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            mapEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeDescribedType(ByteBuf buffer, EncoderState state, DescribedType value) {
        if (value == null) {
            writeNull(buffer, state);
        } else {
            unknownTypeEncoder.writeType(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, boolean[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, byte[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, short[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, int[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, long[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, float[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, double[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, char[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Object[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal32[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal64[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Decimal128[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Symbol[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedByte[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedShort[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedInteger[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UnsignedLong[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, UUID[] value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
        } else {
            arrayEncoder.writeArray(buffer, state, value);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void writeObject(ByteBuf buffer, EncoderState state, Object value) {
        if (value == null) {
            nullEncoder.writeType(buffer, state, null);
            return;
        }

        TypeEncoder encoder = typeEncoders.get(value.getClass());

        if (encoder == null) {
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
                writeList(buffer, state, (List) value);
            } else if (value instanceof Map) {
                writeMap(buffer, state, (Map) value);
            } else if (value instanceof DescribedType) {
                writeDescribedType(buffer, state, (DescribedType) value);
            } else {
                throw new IllegalArgumentException(
                    "Do not know how to write Objects of class " + value.getClass().getName());
            }
        } else {
            encoder.writeType(buffer, state, value);
        }
    }

    @Override
    public <V> ProtonEncoder registerTypeEncoder(TypeEncoder<V> encoder) {
        typeEncoders.put(encoder.getTypeClass(), encoder);
        return this;
    }

    @Override
    public TypeEncoder<?> getTypeEncoder(Object value) {
        if (value == null) {
            return nullEncoder;
        } else {
            return getTypeEncoder(value.getClass());
        }
    }

    @Override
    public TypeEncoder<?> getTypeEncoder(Class<?> typeClass) {
        TypeEncoder<?> encoder = typeEncoders.get(typeClass);

        if (encoder == null) {
            if (typeClass.isArray()) {
                encoder = arrayEncoder;
            } else {
                if (List.class.isAssignableFrom(typeClass)) {
                    encoder = listEncoder;
                } else if (Map.class.isAssignableFrom(typeClass)) {
                    encoder = mapEncoder;
                } else if (DescribedType.class.isAssignableFrom(typeClass)) {
                    encoder = unknownTypeEncoder;
                }
            }

            typeEncoders.put(typeClass, encoder);
        }

        return encoder;
    }
}
