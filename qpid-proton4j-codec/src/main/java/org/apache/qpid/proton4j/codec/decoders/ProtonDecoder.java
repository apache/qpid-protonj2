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
package org.apache.qpid.proton4j.codec.decoders;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Decimal128;
import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Array32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Array8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Binary32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Binary8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.BooleanFalseTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.BooleanTrueTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.BooleanTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ByteTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.CharacterTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Decimal128TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Decimal32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Decimal64TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.DoubleTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.FloatTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Integer32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Integer8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.List0TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.List32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.List8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Long8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.LongTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Map32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Map8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.NullTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ShortTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.String32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.String8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Symbol32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Symbol8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.TimestampTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UUIDTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedByteTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedInteger0TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedInteger32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedInteger8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedLong0TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedLong64TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedLong8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedShortTypeDecoder;

/**
 * The default AMQP Decoder implementation.
 */
public final class ProtonDecoder implements Decoder {

    // The decoders for primitives are fixed and cannot be altered by users who want
    // to register custom decoders.  The decoders created here are stateless and can be
    // made static to reduce overhead of creating Decoder instances.
    private static final PrimitiveTypeDecoder<?>[] primitiveDecoders = new PrimitiveTypeDecoder[256];

    static {
        primitiveDecoders[EncodingCodes.BOOLEAN & 0xFF] = new BooleanTypeDecoder();
        primitiveDecoders[EncodingCodes.BOOLEAN_TRUE & 0xFF] = new BooleanTrueTypeDecoder();
        primitiveDecoders[EncodingCodes.BOOLEAN_FALSE & 0xFF] = new BooleanFalseTypeDecoder();
        primitiveDecoders[EncodingCodes.VBIN8 & 0xFF] = new Binary8TypeDecoder();
        primitiveDecoders[EncodingCodes.VBIN32 & 0xFF] = new Binary32TypeDecoder();
        primitiveDecoders[EncodingCodes.BYTE & 0xFF] = new ByteTypeDecoder();
        primitiveDecoders[EncodingCodes.CHAR & 0xFF] = new CharacterTypeDecoder();
        primitiveDecoders[EncodingCodes.DECIMAL32 & 0xFF] = new Decimal32TypeDecoder();
        primitiveDecoders[EncodingCodes.DECIMAL64 & 0xFF] = new Decimal64TypeDecoder();
        primitiveDecoders[EncodingCodes.DECIMAL128 & 0xFF] = new Decimal128TypeDecoder();
        primitiveDecoders[EncodingCodes.DOUBLE & 0xFF] = new DoubleTypeDecoder();
        primitiveDecoders[EncodingCodes.FLOAT & 0xFF] = new FloatTypeDecoder();
        primitiveDecoders[EncodingCodes.NULL & 0xFF] = new NullTypeDecoder();
        primitiveDecoders[EncodingCodes.SHORT & 0xFF] = new ShortTypeDecoder();
        primitiveDecoders[EncodingCodes.SMALLINT & 0xFF] = new Integer8TypeDecoder();
        primitiveDecoders[EncodingCodes.INT & 0xFF] = new Integer32TypeDecoder();
        primitiveDecoders[EncodingCodes.SMALLLONG & 0xFF] = new Long8TypeDecoder();
        primitiveDecoders[EncodingCodes.LONG & 0xFF] = new LongTypeDecoder();
        primitiveDecoders[EncodingCodes.UBYTE & 0xFF] = new UnsignedByteTypeDecoder();
        primitiveDecoders[EncodingCodes.USHORT & 0xFF] = new UnsignedShortTypeDecoder();
        primitiveDecoders[EncodingCodes.UINT0 & 0xFF] = new UnsignedInteger0TypeDecoder();
        primitiveDecoders[EncodingCodes.SMALLUINT & 0xFF] = new UnsignedInteger8TypeDecoder();
        primitiveDecoders[EncodingCodes.UINT & 0xFF] = new UnsignedInteger32TypeDecoder();
        primitiveDecoders[EncodingCodes.ULONG0 & 0xFF] = new UnsignedLong0TypeDecoder();
        primitiveDecoders[EncodingCodes.SMALLULONG & 0xFF] = new UnsignedLong8TypeDecoder();
        primitiveDecoders[EncodingCodes.ULONG & 0xFF] = new UnsignedLong64TypeDecoder();
        primitiveDecoders[EncodingCodes.STR8 & 0xFF] = new String8TypeDecoder();
        primitiveDecoders[EncodingCodes.STR32 & 0xFF] = new String32TypeDecoder();
        primitiveDecoders[EncodingCodes.SYM8 & 0xFF] = new Symbol8TypeDecoder();
        primitiveDecoders[EncodingCodes.SYM32 & 0xFF] = new Symbol32TypeDecoder();
        primitiveDecoders[EncodingCodes.UUID & 0xFF] = new UUIDTypeDecoder();
        primitiveDecoders[EncodingCodes.TIMESTAMP & 0xFF] = new TimestampTypeDecoder();
        primitiveDecoders[EncodingCodes.LIST0 & 0xFF] = new List0TypeDecoder();
        primitiveDecoders[EncodingCodes.LIST8 & 0xFF] = new List8TypeDecoder();
        primitiveDecoders[EncodingCodes.LIST32 & 0xFF] = new List32TypeDecoder();
        primitiveDecoders[EncodingCodes.MAP8 & 0xFF] = new Map8TypeDecoder();
        primitiveDecoders[EncodingCodes.MAP32 & 0xFF] = new Map32TypeDecoder();
        primitiveDecoders[EncodingCodes.ARRAY8 & 0xFF] = new Array8TypeDecoder();
        primitiveDecoders[EncodingCodes.ARRAY32 & 0xFF] = new Array32TypeDecoder();

        // Initialize the locally used primitive type decoders for the main API
        symbol8Decoder = (Symbol8TypeDecoder) primitiveDecoders[EncodingCodes.SYM8 & 0xFF];
        symbol32Decoder = (Symbol32TypeDecoder) primitiveDecoders[EncodingCodes.SYM32 & 0xFF];
        binary8Decoder = (Binary8TypeDecoder) primitiveDecoders[EncodingCodes.VBIN8 & 0xFF];
        binary32Decoder = (Binary32TypeDecoder) primitiveDecoders[EncodingCodes.VBIN32 & 0xFF];
        list8Decoder = (List8TypeDecoder) primitiveDecoders[EncodingCodes.LIST8 & 0xFF];
        list32Decoder = (List32TypeDecoder) primitiveDecoders[EncodingCodes.LIST32 & 0xFF];
        map8Decoder = (Map8TypeDecoder) primitiveDecoders[EncodingCodes.MAP8 & 0xFF];
        map32Decoder = (Map32TypeDecoder) primitiveDecoders[EncodingCodes.MAP32 & 0xFF];
        string32Decoder = (String32TypeDecoder) primitiveDecoders[EncodingCodes.STR32 & 0xFF];
        string8Decoder = (String8TypeDecoder) primitiveDecoders[EncodingCodes.STR8 & 0xFF];
    }

    // Registry of decoders for described types which can be updated with user defined
    // decoders as well as the default decoders.
    private Map<Object, DescribedTypeDecoder<?>> describedTypeDecoders = new HashMap<>();

    // Quick access to decoders that handle AMQP types like Transfer, Properties etc.
    private final DescribedTypeDecoder<?>[] amqpTypeDecoders = new DescribedTypeDecoder[256];

    // Internal Decoders used to prevent user to access Proton specific decoding methods
    private static final Symbol8TypeDecoder symbol8Decoder;
    private static final Symbol32TypeDecoder symbol32Decoder;
    private static final Binary8TypeDecoder binary8Decoder;
    private static final Binary32TypeDecoder binary32Decoder;
    private static final List8TypeDecoder list8Decoder;
    private static final List32TypeDecoder list32Decoder;
    private static final Map8TypeDecoder map8Decoder;
    private static final Map32TypeDecoder map32Decoder;
    private static final String8TypeDecoder string8Decoder;
    private static final String32TypeDecoder string32Decoder;

    @Override
    public ProtonDecoderState newDecoderState() {
        return new ProtonDecoderState(this);
    }

    @Override
    public Object readObject(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = readNextTypeDecoder(buffer, state);

        if (decoder == null) {
            throw new IOException("Unknown type constructor in encoded bytes");
        }

        return decoder.readValue(buffer, state);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws IOException {
        Object result = readObject(buffer, state);

        if (result == null) {
            return null;
        } else if (clazz.isAssignableFrom(result.getClass())) {
            return (T) result;
        } else {
            throw signalUnexpectedType(result, clazz);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] readMultiple(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws IOException {
        Object val = readObject(buffer, state);

        if (val == null) {
            return null;
        } else if (val.getClass().isArray()) {
            if (clazz.isAssignableFrom(val.getClass().getComponentType())) {
                return (T[]) val;
            } else {
                throw signalUnexpectedType(val, Array.newInstance(clazz, 0).getClass());
            }
        } else if (clazz.isAssignableFrom(val.getClass())) {
            T[] array = (T[]) Array.newInstance(clazz, 1);
            array[0] = (T) val;
            return array;
        } else {
            throw signalUnexpectedType(val, Array.newInstance(clazz, 0).getClass());
        }
    }

    @Override
    public TypeDecoder<?> readNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws IOException {
        int encodingCode = buffer.readByte() & 0xFF;

        if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR) {
            buffer.markReadIndex();
            try {
                final long result = readUnsignedLong(buffer, state, amqpTypeDecoders.length);

                if (result > 0 && result < amqpTypeDecoders.length && amqpTypeDecoders[(int) result] != null) {
                    return amqpTypeDecoders[(int) result];
                } else {
                    buffer.resetReadIndex();
                    return slowReadNextTypeDecoder(buffer, state);
                }
            } catch (Exception e) {
                buffer.resetReadIndex();
                return slowReadNextTypeDecoder(buffer, state);
            }
        } else {
            return primitiveDecoders[encodingCode];
        }
    }

    private TypeDecoder<?> slowReadNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws IOException {
        Object descriptor;
        buffer.markReadIndex();
        try {
            descriptor = readUnsignedLong(buffer, state);
        } catch (Exception e) {
            buffer.resetReadIndex();
            descriptor = readObject(buffer, state);
        }

        TypeDecoder<?> typeDecoder = describedTypeDecoders.get(descriptor);
        if (typeDecoder == null) {
            typeDecoder = handleUnknownDescribedType(descriptor);
        }

        return typeDecoder;
    }

    @Override
    public TypeDecoder<?> peekNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws IOException {
        buffer.markReadIndex();
        try {
            return readNextTypeDecoder(buffer, state);
        } finally {
            buffer.resetReadIndex();
        }
    }

    @Override
    public <V> ProtonDecoder registerDescribedTypeDecoder(DescribedTypeDecoder<V> decoder) {
        DescribedTypeDecoder<?> describedTypeDecoder = decoder;

        // Cache AMQP type decoders in the quick lookup array.
        if (decoder.getDescriptorCode().compareTo(amqpTypeDecoders.length) < 0) {
            amqpTypeDecoders[decoder.getDescriptorCode().intValue()] = decoder;
        }

        describedTypeDecoders.put(describedTypeDecoder.getDescriptorCode(), describedTypeDecoder);
        describedTypeDecoders.put(describedTypeDecoder.getDescriptorSymbol(), describedTypeDecoder);

        return this;
    }

    @Override
    public Boolean readBoolean(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BOOLEAN_TRUE:
                return Boolean.TRUE;
            case EncodingCodes.BOOLEAN_FALSE:
                return Boolean.FALSE;
            case EncodingCodes.BOOLEAN:
                return buffer.readByte() == 0 ? Boolean.FALSE : Boolean.TRUE;
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected boolean type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public boolean readBoolean(ProtonBuffer buffer, DecoderState state, boolean defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BOOLEAN_TRUE:
                return true;
            case EncodingCodes.BOOLEAN_FALSE:
                return false;
            case EncodingCodes.BOOLEAN:
                return buffer.readByte() == 0 ? false : true;
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected boolean type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Byte readByte(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return buffer.readByte();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected byte type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public byte readByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return buffer.readByte();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected byte type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public UnsignedByte readUnsignedByte(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UBYTE:
                return UnsignedByte.valueOf(buffer.readByte());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected unsigned byte type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public byte readUnsignedByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UBYTE:
                return buffer.readByte();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected unsigned byte type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Character readCharacter(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.CHAR:
                return Character.valueOf((char) (buffer.readInt() & 0xffff));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Character type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public char readCharacter(ProtonBuffer buffer, DecoderState state, char defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.CHAR:
                return (char) (buffer.readInt() & 0xffff);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Character type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Decimal32 readDecimal32(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL32:
                return new Decimal32(buffer.readInt());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal32 type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Decimal64 readDecimal64(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL64:
                return new Decimal64(buffer.readLong());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal64 type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Decimal128 readDecimal128(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL128:
                return new Decimal128(buffer.readLong(), buffer.readLong());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal128 type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Short readShort(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SHORT:
                return buffer.readShort();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Short type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public short readShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SHORT:
                return buffer.readShort();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Short type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public UnsignedShort readUnsignedShort(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return UnsignedShort.valueOf(buffer.readShort());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UnsignedShort type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public short readUnsignedShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return buffer.readShort();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected UnsignedShort type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public int readUnsignedShort(ProtonBuffer buffer, DecoderState state, int defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return buffer.readShort() & 0xffff;
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected UnsignedShort type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Integer readInteger(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SMALLINT:
                return buffer.readByte() & 0xff;
            case EncodingCodes.INT:
                return buffer.readInt();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Integer type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public int readInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SMALLINT:
                return buffer.readByte() & 0xff;
            case EncodingCodes.INT:
                return buffer.readInt();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Integer type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public UnsignedInteger readUnsignedInteger(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return UnsignedInteger.ZERO;
            case EncodingCodes.SMALLUINT:
                return UnsignedInteger.valueOf((buffer.readByte()) & 0xff);
            case EncodingCodes.UINT:
                return UnsignedInteger.valueOf((buffer.readInt()));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UnsignedInteger type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public int readUnsignedInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return 0;
            case EncodingCodes.SMALLUINT:
                return buffer.readByte() & 0xff;
            case EncodingCodes.UINT:
                return buffer.readInt();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected UnsignedInteger type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public long readUnsignedInteger(ProtonBuffer buffer, DecoderState state, long defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return 0;
            case EncodingCodes.SMALLUINT:
                return buffer.readByte() & 0xff;
            case EncodingCodes.UINT:
                return buffer.readInt() & 0xffffffffl;
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected UnsignedInteger type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Long readLong(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SMALLLONG:
                return (long) buffer.readByte() & 0xff;
            case EncodingCodes.LONG:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Long type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public long readLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SMALLLONG:
                return (long) buffer.readByte() & 0xff;
            case EncodingCodes.LONG:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Long type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public UnsignedLong readUnsignedLong(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.ULONG0:
                return UnsignedLong.ZERO;
            case EncodingCodes.SMALLULONG:
                return UnsignedLong.valueOf((buffer.readByte() & 0xff));
            case EncodingCodes.ULONG:
                return UnsignedLong.valueOf((buffer.readLong()));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Long type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public long readUnsignedLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.ULONG0:
                return 0l;
            case EncodingCodes.SMALLULONG:
                return (buffer.readByte() & 0xff);
            case EncodingCodes.ULONG:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Long type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Float readFloat(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.FLOAT:
                return buffer.readFloat();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Float type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public float readFloat(ProtonBuffer buffer, DecoderState state, float defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.FLOAT:
                return buffer.readFloat();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Float type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Double readDouble(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DOUBLE:
                return buffer.readDouble();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Double type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public double readDouble(ProtonBuffer buffer, DecoderState state, double defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DOUBLE:
                return buffer.readDouble();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Double type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Binary readBinary(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return binary8Decoder.readValue(buffer, state);
            case EncodingCodes.VBIN32:
                return binary32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Binary type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public ProtonBuffer readBinaryAsBuffer(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return binary8Decoder.readValueAsBuffer(buffer, state);
            case EncodingCodes.VBIN32:
                return binary32Decoder.readValueAsBuffer(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Binary type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public DeliveryTag readDeliveryTag(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return new DeliveryTag.ProtonDeliveryTag(binary8Decoder.readValueAsArray(buffer, state));
            case EncodingCodes.VBIN32:
                return new DeliveryTag.ProtonDeliveryTag(binary32Decoder.readValueAsArray(buffer, state));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Binary type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public String readString(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.STR8:
                return string8Decoder.readValue(buffer, state);
            case EncodingCodes.STR32:
                return string32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected String type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Symbol readSymbol(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SYM8:
                return symbol8Decoder.readValue(buffer, state);
            case EncodingCodes.SYM32:
                return symbol32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Symbol type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public String readSymbol(ProtonBuffer buffer, DecoderState state, String defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SYM8:
                return symbol8Decoder.readString(buffer, state);
            case EncodingCodes.SYM32:
                return symbol32Decoder.readString(buffer, state);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Symbol type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public Long readTimestamp(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.TIMESTAMP:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Timestamp type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public long readTimestamp(ProtonBuffer buffer, DecoderState state, long defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.TIMESTAMP:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected Timestamp type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @Override
    public UUID readUUID(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UUID:
                return new UUID(buffer.readLong(), buffer.readLong());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UUID type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Map<K, V> readMap(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.MAP8:
                return (Map<K, V>) map8Decoder.readValue(buffer, state);
            case EncodingCodes.MAP32:
                return (Map<K, V>) map32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Map type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> List<V> readList(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.LIST0:
                return Collections.emptyList();
            case EncodingCodes.LIST8:
                return (List<V>) list8Decoder.readValue(buffer, state);
            case EncodingCodes.LIST32:
                return (List<V>) list32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected List type but found encoding: " + (encodingCode & 0xFF));
        }
    }

    private ClassCastException signalUnexpectedType(final Object val, Class<?> clazz) {
        return new ClassCastException("Unexpected type " + val.getClass().getName() +
                                      ". Expected " + clazz.getName() + ".");
    }

    private TypeDecoder<?> handleUnknownDescribedType(final Object descriptor) {
        TypeDecoder<?> typeDecoder = new UnknownDescribedTypeDecoder() {

            @Override
            public Object getDescriptor() {
                return descriptor;
            }
        };

        describedTypeDecoders.put(descriptor, (UnknownDescribedTypeDecoder) typeDecoder);

        return typeDecoder;
    }
}
