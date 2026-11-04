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
package org.apache.qpid.protonj2.codec.decoders;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeEOFException;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.Decoder;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.DescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.AbstractSymbolTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Array32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Array8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Binary32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Binary8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.BooleanFalseTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.BooleanTrueTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.BooleanTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ByteTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.CharacterTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Decimal128TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Decimal32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Decimal64TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.DoubleTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.FloatTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Integer32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Integer8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.List0TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.List32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.List8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Long8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.LongTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Map32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Map8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.NullTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.SaslSymbol32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.SaslSymbol8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.ShortTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.String32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.String8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Symbol32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.Symbol8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.TimestampTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UUIDTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UnsignedByteTypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UnsignedInteger0TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UnsignedInteger32TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UnsignedInteger8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UnsignedLong0TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UnsignedLong64TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UnsignedLong8TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.primitives.UnsignedShortTypeDecoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.Decimal128;
import org.apache.qpid.protonj2.types.Decimal32;
import org.apache.qpid.protonj2.types.Decimal64;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedByte;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.UnsignedLong;
import org.apache.qpid.protonj2.types.UnsignedShort;

/**
 * The default AMQP Decoder implementation.
 */
public class ProtonDecoder implements Decoder {

    // The number of unknown described type decoders that are cached for performance
    // but limited for memory protection.
    public static final int UNKNOWN_DESCRIBED_TYPES_CACHE_LIMIT = 16;

    // If the descriptor for an unknown described type is a Symbol then it must have a
    // length shorter than this value to be cached for future lookup.
    public static final int UNKNOWN_DESCRIBED_TYPE_DESCRIPTOR_SIZE_LIMIT = 64;

    // The decoders for primitives are fixed and cannot be altered by users who want
    // to register custom decoders.  The decoders created here are stateless and can be
    // made static to reduce overhead of creating Decoder instances.
    private static final PrimitiveTypeDecoder<?>[] primitiveDecoders = new PrimitiveTypeDecoder[256];

    // Mode value used to trigger setup of the decoder on create, in the SASL mode the
    // decoders represent the special case where data is meant only for the SASL exchange
    protected enum DecoderMode {
        SASL,
        DEFAULT
    }

    static {
        primitiveDecoders[EncodingCodes.BOOLEAN & 0xFF] = BooleanTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.BOOLEAN_TRUE & 0xFF] = BooleanTrueTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.BOOLEAN_FALSE & 0xFF] = BooleanFalseTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.VBIN8 & 0xFF] = Binary8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.VBIN32 & 0xFF] = Binary32TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.BYTE & 0xFF] = ByteTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.CHAR & 0xFF] = CharacterTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.DECIMAL32 & 0xFF] = Decimal32TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.DECIMAL64 & 0xFF] = Decimal64TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.DECIMAL128 & 0xFF] = Decimal128TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.DOUBLE & 0xFF] = DoubleTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.FLOAT & 0xFF] = FloatTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.NULL & 0xFF] = NullTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.SHORT & 0xFF] = ShortTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.SMALLINT & 0xFF] = Integer8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.INT & 0xFF] = Integer32TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.SMALLLONG & 0xFF] = Long8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.LONG & 0xFF] = LongTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.UBYTE & 0xFF] = UnsignedByteTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.USHORT & 0xFF] = UnsignedShortTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.UINT0 & 0xFF] = UnsignedInteger0TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.SMALLUINT & 0xFF] = UnsignedInteger8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.UINT & 0xFF] = UnsignedInteger32TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.ULONG0 & 0xFF] = UnsignedLong0TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.SMALLULONG & 0xFF] = UnsignedLong8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.ULONG & 0xFF] = UnsignedLong64TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.STR8 & 0xFF] = String8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.STR32 & 0xFF] = String32TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.SYM8 & 0xFF] = Symbol8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.SYM32 & 0xFF] = Symbol32TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.UUID & 0xFF] = UUIDTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.TIMESTAMP & 0xFF] = TimestampTypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.LIST0 & 0xFF] = List0TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.LIST8 & 0xFF] = List8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.LIST32 & 0xFF] = List32TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.MAP8 & 0xFF] = Map8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.MAP32 & 0xFF] = Map32TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.ARRAY8 & 0xFF] = Array8TypeDecoder.INSTANCE;
        primitiveDecoders[EncodingCodes.ARRAY32 & 0xFF] = Array32TypeDecoder.INSTANCE;

        // Initialize the locally used primitive type decoders for the main API
        symbol8Decoder = (Symbol8TypeDecoder) primitiveDecoders[EncodingCodes.SYM8 & 0xFF];
        symbol32Decoder = (Symbol32TypeDecoder) primitiveDecoders[EncodingCodes.SYM32 & 0xFF];
        saslSymbol8Decoder = SaslSymbol8TypeDecoder.INSTANCE;
        saslSymbol32Decoder = SaslSymbol32TypeDecoder.INSTANCE;
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
    private final Map<Object, DescribedTypeDecoder<?>> describedTypeDecoders = new ConcurrentHashMap<>();

    // Registry of decoders for described types which are not registered which can be updated with a
    // limited number of cached decoders to speed up processing
    private final Map<Object, UnknownDescribedTypeDecoder> unknownDescribedTypeDecoders = new ConcurrentHashMap<>();

    // Quick access to decoders that handle AMQP types like Transfer, Properties etc.
    private final DescribedTypeDecoder<?>[] amqpTypeDecoders = new DescribedTypeDecoder[256];

    // Provides a mechanism for users to register a decoder for reserved descriptors, the library
    // does not allow this normally but some users may wish to decode reserved types for messages
    // or other implementation specific use cases.
    private final BiFunction<ProtonBuffer, DecoderState, Object> reservedDescriptorDecoder;

    // Internal Decoders used to prevent user to access Proton specific decoding methods
    private static final Symbol8TypeDecoder symbol8Decoder;
    private static final Symbol32TypeDecoder symbol32Decoder;
    private static final SaslSymbol8TypeDecoder saslSymbol8Decoder;
    private static final SaslSymbol32TypeDecoder saslSymbol32Decoder;
    private static final Binary8TypeDecoder binary8Decoder;
    private static final Binary32TypeDecoder binary32Decoder;
    private static final List8TypeDecoder list8Decoder;
    private static final List32TypeDecoder list32Decoder;
    private static final Map8TypeDecoder map8Decoder;
    private static final Map32TypeDecoder map32Decoder;
    private static final String8TypeDecoder string8Decoder;
    private static final String32TypeDecoder string32Decoder;

    private final DecoderMode decoderMode;
    private final PrimitiveTypeDecoder<?>[] localPrimitiveDecoders;
    private final AbstractSymbolTypeDecoder localSymbol8Decoder;
    private final AbstractSymbolTypeDecoder localSymbol32Decoder;

    private ProtonDecoderState singleThreadedState;

    public ProtonDecoder() {
        this(DecoderMode.DEFAULT, null);
    }

    public ProtonDecoder(BiFunction<ProtonBuffer, DecoderState, Object> descriptorDecoder) {
        this(DecoderMode.DEFAULT, descriptorDecoder);
    }

    public ProtonDecoder(DecoderMode mode) {
        this(mode, null);
    }

    public ProtonDecoder(DecoderMode mode, BiFunction<ProtonBuffer, DecoderState, Object> descriptorDecoder) {
        decoderMode = mode;
        reservedDescriptorDecoder = descriptorDecoder;

        if (DecoderMode.SASL.equals(decoderMode)) {
            localSymbol8Decoder = saslSymbol8Decoder;
            localSymbol32Decoder = saslSymbol32Decoder;
            localPrimitiveDecoders = Arrays.copyOfRange(primitiveDecoders, 0, primitiveDecoders.length);
            localPrimitiveDecoders[EncodingCodes.SYM8 & 0xFF] = saslSymbol8Decoder;
            localPrimitiveDecoders[EncodingCodes.SYM32 & 0xFF] = saslSymbol32Decoder;
        } else {
            localSymbol8Decoder = symbol8Decoder;
            localSymbol32Decoder = symbol32Decoder;
            localPrimitiveDecoders = primitiveDecoders;
        }
    }

    @Override
    public ProtonDecoderState newDecoderState() {
        return new ProtonDecoderState(this);
    }

    @Override
    public ProtonDecoderState getCachedDecoderState() {
        ProtonDecoderState state = singleThreadedState;
        if (state == null) {
            singleThreadedState = state = newDecoderState();
        }

        return state.reset();
    }

    @Override
    public Object readObject(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final TypeDecoder<?> decoder = readNextTypeDecoder(buffer, state);

        if (decoder == null) {
            throw new DecodeException("Unknown type constructor in encoded bytes");
        }

        return decoder.readValue(buffer, state);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws DecodeException {
        final TypeDecoder<?> decoder = readNextTypeDecoder(buffer, state);

        if (decoder.isNull()) {
            return null;
        } else if (decoder.isArrayType()) {
            return (T) ((PrimitiveArrayTypeDecoder) decoder).readValue(buffer, state, clazz);
        } else if (clazz.isAssignableFrom(decoder.getTypeClass())) {
            return (T) decoder.readValue(buffer, state);
        } else {
            throw signalUnexpectedType(decoder.getTypeClass(), Array.newInstance(clazz, 0).getClass());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] readMultiple(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws DecodeException {
        final TypeDecoder<?> decoder = readNextTypeDecoder(buffer, state);

        if (decoder.isNull()) {
            return null;
        } else if (decoder.isArrayType()) {
            return (T[]) ((PrimitiveArrayTypeDecoder) decoder).readValue(buffer, state, clazz);
        } else if (clazz.isAssignableFrom(decoder.getTypeClass())) {
            T[] array = (T[]) Array.newInstance(clazz, 1);
            array[0] = (T) decoder.readValue(buffer, state);
            return array;
        } else {
            throw signalUnexpectedType(decoder.getTypeClass(), Array.newInstance(clazz, 0).getClass());
        }
    }

    @Override
    public TypeDecoder<?> readNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int encodingCode = readEncodingCode(buffer) & 0xff;

        if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR) {
            final int readMark = buffer.getReadOffset();
            try {
                final long result = readUnsignedLong(buffer, state, amqpTypeDecoders.length);

                if (result > 0 && result < amqpTypeDecoders.length && amqpTypeDecoders[(int) result] != null) {
                    return amqpTypeDecoders[(int) result];
                } else {
                    buffer.setReadOffset(readMark);
                    return slowReadNextTypeDecoder(buffer, state);
                }
            } catch (Exception e) {
                buffer.setReadOffset(readMark);
                return slowReadNextTypeDecoder(buffer, state);
            }
        } else {
            return localPrimitiveDecoders[encodingCode];
        }
    }

    private TypeDecoder<?> slowReadNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        Object descriptor;
        final int readMark = buffer.getReadOffset();

        try {
            descriptor = readUnsignedLong(buffer, state);
        } catch (Exception e) {
            buffer.setReadOffset(readMark);
            try {
                descriptor = readSymbol(buffer, state);
            } catch (Exception ex) {
                if (decoderMode != DecoderMode.SASL) {
                    if (reservedDescriptorDecoder != null) {
                        state.increaseDepth();
                        try {
                            descriptor = reservedDescriptorDecoder.apply(buffer, state);
                        } finally {
                            state.decreaseDepth();
                        }
                    } else {
                        throw new DecodeException(String.format(
                            "Cannot decode a type that is using a reserved type descriptor: %s", peekNextTypeDecoder(buffer, state).getTypeClass()));
                    }
                } else {
                    throw new DecodeException("Cannot decode reserved descriptor type in SASL mode.");
                }
            }
        }

        TypeDecoder<?> typeDecoder = describedTypeDecoders.get(descriptor);
        if (typeDecoder == null) {
            typeDecoder = unknownDescribedTypeDecoders.get(descriptor);
            if (typeDecoder == null) {
                typeDecoder = handleUnknownDescribedType(descriptor);
            }
        }

        return typeDecoder;
    }

    @Override
    public TypeDecoder<?> peekNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final int readMark = buffer.getReadOffset();
        try {
            return readNextTypeDecoder(buffer, state);
        } finally {
            buffer.setReadOffset(readMark);
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

        decoder.decoderRegistered(this);

        return this;
    }

    @Override
    public Boolean readBoolean(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

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
                throw new DecodeException("Expected Boolean type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public boolean readBoolean(ProtonBuffer buffer, DecoderState state, boolean defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

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
                throw new DecodeException("Expected Boolean type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Byte readByte(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return buffer.readByte();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public byte readByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return buffer.readByte();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedByte readUnsignedByte(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.UBYTE:
                return UnsignedByte.valueOf(buffer.readByte());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Unsigned Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public byte readUnsignedByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.UBYTE:
                return buffer.readByte();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Character readCharacter(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.CHAR:
                return Character.valueOf((char) (buffer.readInt() & 0xffff));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Character type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public char readCharacter(ProtonBuffer buffer, DecoderState state, char defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.CHAR:
                return (char) (buffer.readInt() & 0xffff);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Character type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal32 readDecimal32(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.DECIMAL32:
                return new Decimal32(buffer.readInt());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Decimal32 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal64 readDecimal64(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.DECIMAL64:
                return new Decimal64(buffer.readLong());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Decimal64 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal128 readDecimal128(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.DECIMAL128:
                return new Decimal128(buffer.readLong(), buffer.readLong());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Decimal128 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Short readShort(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.SHORT:
                return buffer.readShort();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public short readShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.SHORT:
                return buffer.readShort();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedShort readUnsignedShort(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return UnsignedShort.valueOf(buffer.readShort());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Unsigned Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public short readUnsignedShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return buffer.readShort();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public int readUnsignedShort(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return buffer.readShort() & 0xffff;
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Integer readInteger(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.SMALLINT:
                return (int) buffer.readByte();
            case EncodingCodes.INT:
                return buffer.readInt();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public int readInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.SMALLINT:
                return buffer.readByte();
            case EncodingCodes.INT:
                return buffer.readInt();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedInteger readUnsignedInteger(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

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
                throw new DecodeException("Expected Unsigned Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedInteger readUnsignedInteger(ProtonBuffer buffer, DecoderState state, UnsignedInteger defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return UnsignedInteger.ZERO;
            case EncodingCodes.SMALLUINT:
                return UnsignedInteger.valueOf((buffer.readByte()) & 0xff);
            case EncodingCodes.UINT:
                return UnsignedInteger.valueOf((buffer.readInt()));
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public int readUnsignedInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

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
                throw new DecodeException("Expected Unsigned Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readUnsignedInteger(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

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
                throw new DecodeException("Expected Unsigned Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Long readLong(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.SMALLLONG:
                return (long) buffer.readByte();
            case EncodingCodes.LONG:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.SMALLLONG:
                return buffer.readByte();
            case EncodingCodes.LONG:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedLong readUnsignedLong(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

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
                throw new DecodeException("Expected Unsigned Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readUnsignedLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

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
                throw new DecodeException("Expected Unsigned Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Float readFloat(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.FLOAT:
                return buffer.readFloat();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Float type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public float readFloat(ProtonBuffer buffer, DecoderState state, float defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.FLOAT:
                return buffer.readFloat();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Float type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Double readDouble(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.DOUBLE:
                return buffer.readDouble();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Double type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public double readDouble(ProtonBuffer buffer, DecoderState state, double defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.DOUBLE:
                return buffer.readDouble();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Double type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Binary readBinary(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return binary8Decoder.readValue(buffer, state);
            case EncodingCodes.VBIN32:
                return binary32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Binary type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public ProtonBuffer readBinaryAsBuffer(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return binary8Decoder.readValueAsBuffer(buffer, state);
            case EncodingCodes.VBIN32:
                return binary32Decoder.readValueAsBuffer(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Binary type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public DeliveryTag readDeliveryTag(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return new DeliveryTag.ProtonDeliveryTag(binary8Decoder.readValueAsArray(buffer, state));
            case EncodingCodes.VBIN32:
                return new DeliveryTag.ProtonDeliveryTag(binary32Decoder.readValueAsArray(buffer, state));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Binary type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public String readString(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.STR8:
                return string8Decoder.readValue(buffer, state);
            case EncodingCodes.STR32:
                return string32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected String type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Symbol readSymbol(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.SYM8:
                return localSymbol8Decoder.readValue(buffer, state);
            case EncodingCodes.SYM32:
                return localSymbol32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Symbol type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public String readSymbol(ProtonBuffer buffer, DecoderState state, String defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.SYM8:
                return localSymbol8Decoder.readString(buffer, state);
            case EncodingCodes.SYM32:
                return localSymbol32Decoder.readString(buffer, state);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Symbol type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Long readTimestamp(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.TIMESTAMP:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Timestamp type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readTimestamp(ProtonBuffer buffer, DecoderState state, long defaultValue) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.TIMESTAMP:
                return buffer.readLong();
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Timestamp type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UUID readUUID(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.UUID:
                return new UUID(buffer.readLong(), buffer.readLong());
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected UUID type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Map<K, V> readMap(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

        switch (encodingCode) {
            case EncodingCodes.MAP8:
                return (Map<K, V>) map8Decoder.readValue(buffer, state);
            case EncodingCodes.MAP32:
                return (Map<K, V>) map32Decoder.readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Map type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> List<V> readList(ProtonBuffer buffer, DecoderState state) throws DecodeException {
        final byte encodingCode = readEncodingCode(buffer);

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
                throw new DecodeException("Expected List type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    private static byte readEncodingCode(ProtonBuffer buffer) throws DecodeEOFException {
        try {
            return buffer.readByte();
        } catch (IndexOutOfBoundsException iobe) {
            throw new DecodeEOFException("Read of new type failed because buffer exhausted.", iobe);
        }
    }

    private DecodeException signalUnexpectedType(final Class<?> actual, Class<?> expected) {
        return new DecodeException("Unexpected type " + actual.getName() + ". Expected " + expected.getName() + ".");
    }

    private TypeDecoder<?> handleUnknownDescribedType(final Object descriptor) {
        if (DecoderMode.SASL.equals(decoderMode)) {
            throw new DecodeException("Cannot decode unknown described types from a SASL mode decoder");
        }

        final boolean canCache;

        if (descriptor instanceof Symbol symbol) {
            canCache = symbol.getLength() <= UNKNOWN_DESCRIBED_TYPE_DESCRIPTOR_SIZE_LIMIT;
        } else {
            canCache = descriptor instanceof UnsignedLong;
        }

        final UnknownDescribedTypeDecoder typeDecoder = new UnknownDescribedTypeDecoder() {

            @Override
            public Object getDescriptor() {
                return descriptor;
            }
        };

        if (canCache && unknownDescribedTypeDecoders.size() < UNKNOWN_DESCRIBED_TYPES_CACHE_LIMIT) {
            unknownDescribedTypeDecoders.putIfAbsent(descriptor, typeDecoder);
        }

        return typeDecoder;
    }
}
