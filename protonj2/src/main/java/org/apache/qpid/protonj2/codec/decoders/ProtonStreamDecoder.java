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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
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
public final class ProtonStreamDecoder implements StreamDecoder {

    private static final int STREAM_PEEK_MARK_LIMIT = 64;

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
    private Map<Object, StreamDescribedTypeDecoder<?>> describedTypeDecoders = new HashMap<>();

    // Quick access to decoders that handle AMQP types like Transfer, Properties etc.
    private final StreamDescribedTypeDecoder<?>[] amqpTypeDecoders = new StreamDescribedTypeDecoder[256];

    private ProtonStreamDecoderState singleThreadedState;

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
    public ProtonStreamDecoderState newDecoderState() {
        return new ProtonStreamDecoderState(this);
    }

    @Override
    public ProtonStreamDecoderState getCachedDecoderState() {
        ProtonStreamDecoderState state = singleThreadedState;
        if (state == null) {
            singleThreadedState = state = newDecoderState();
        }

        return state.reset();
    }

    @Override
    public Object readObject(InputStream stream, StreamDecoderState state) throws DecodeException {
        StreamTypeDecoder<?> decoder = readNextTypeDecoder(stream, state);

        if (decoder == null) {
            throw new DecodeException("Unknown type constructor in encoded bytes");
        }

        return decoder.readValue(stream, state);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObject(InputStream stream, StreamDecoderState state, final Class<T> clazz) throws DecodeException {
        Object result = readObject(stream, state);

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
    public <T> T[] readMultiple(InputStream stream, StreamDecoderState state, final Class<T> clazz) throws DecodeException {
        Object val = readObject(stream, state);

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
    public StreamTypeDecoder<?> readNextTypeDecoder(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR) {
            if (stream.markSupported()) {
                stream.mark(STREAM_PEEK_MARK_LIMIT);
                try {
                    final long result = readUnsignedLong(stream, state, amqpTypeDecoders.length);

                    if (result > 0 && result < amqpTypeDecoders.length && amqpTypeDecoders[(int) result] != null) {
                        return amqpTypeDecoders[(int) result];
                    } else {
                        ProtonStreamUtils.reset(stream);
                        return slowReadNextTypeDecoder(stream, state);
                    }
                } catch (Exception e) {
                    ProtonStreamUtils.reset(stream);
                    return slowReadNextTypeDecoder(stream, state);
                }
            } else {
                return slowReadNextTypeDecoder(stream, state);
            }
        } else {
            return primitiveDecoders[encodingCode & 0xff];
        }
    }

    private StreamTypeDecoder<?> slowReadNextTypeDecoder(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);
        final Object descriptor;

        switch (encodingCode) {
            case EncodingCodes.SMALLULONG:
                descriptor = UnsignedLong.valueOf(ProtonStreamUtils.readByte(stream) & 0xffl);
                break;
            case EncodingCodes.ULONG:
                descriptor = UnsignedLong.valueOf(ProtonStreamUtils.readLong(stream));
                break;
            case EncodingCodes.SYM8:
                descriptor = symbol8Decoder.readValue(stream, state);
                break;
            case EncodingCodes.SYM32:
                descriptor = symbol32Decoder.readValue(stream, state);
                break;
            default:
                throw new DecodeException("Expected Descriptor type but found encoding: " + EncodingCodes.toString(encodingCode));
        }

        StreamTypeDecoder<?> StreamTypeDecoder = describedTypeDecoders.get(descriptor);
        if (StreamTypeDecoder == null) {
            StreamTypeDecoder = handleUnknownDescribedType(descriptor);
        }

        return StreamTypeDecoder;
    }

    @Override
    public StreamTypeDecoder<?> peekNextTypeDecoder(InputStream stream, StreamDecoderState state) throws DecodeException {
        if (stream.markSupported()) {
            stream.mark(STREAM_PEEK_MARK_LIMIT);
            try {
                return readNextTypeDecoder(stream, state);
            } finally {
                try {
                    stream.reset();
                } catch (IOException e) {
                    throw new DecodeException("Error while reseting marked stream", e);
                }
            }
        } else {
            throw new UnsupportedOperationException("The provided stream doesn't support stream marks");
        }
    }

    @Override
    public <V> ProtonStreamDecoder registerDescribedTypeDecoder(StreamDescribedTypeDecoder<V> decoder) {
        StreamDescribedTypeDecoder<?> describedTypeDecoder = decoder;

        // Cache AMQP type decoders in the quick lookup array.
        if (decoder.getDescriptorCode().compareTo(amqpTypeDecoders.length) < 0) {
            amqpTypeDecoders[decoder.getDescriptorCode().intValue()] = decoder;
        }

        describedTypeDecoders.put(describedTypeDecoder.getDescriptorCode(), describedTypeDecoder);
        describedTypeDecoders.put(describedTypeDecoder.getDescriptorSymbol(), describedTypeDecoder);

        return this;
    }

    @Override
    public Boolean readBoolean(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.BOOLEAN_TRUE:
                return Boolean.TRUE;
            case EncodingCodes.BOOLEAN_FALSE:
                return Boolean.FALSE;
            case EncodingCodes.BOOLEAN:
                return ProtonStreamUtils.readByte(stream) == 0 ? Boolean.FALSE : Boolean.TRUE;
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Boolean type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public boolean readBoolean(InputStream stream, StreamDecoderState state, boolean defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.BOOLEAN_TRUE:
                return true;
            case EncodingCodes.BOOLEAN_FALSE:
                return false;
            case EncodingCodes.BOOLEAN:
                return ProtonStreamUtils.readByte(stream) == 0 ? false : true;
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Boolean type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Byte readByte(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return ProtonStreamUtils.readByte(stream);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public byte readByte(InputStream stream, StreamDecoderState state, byte defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return ProtonStreamUtils.readByte(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedByte readUnsignedByte(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.UBYTE:
                return UnsignedByte.valueOf(ProtonStreamUtils.readByte(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Unsigned Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public byte readUnsignedByte(InputStream stream, StreamDecoderState state, byte defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.UBYTE:
                return ProtonStreamUtils.readByte(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Byte type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Character readCharacter(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.CHAR:
                return Character.valueOf((char) (ProtonStreamUtils.readInt(stream) & 0xFFFF));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Character type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public char readCharacter(InputStream stream, StreamDecoderState state, char defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.CHAR:
                return (char) (ProtonStreamUtils.readInt(stream) & 0xFFFF);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Character type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal32 readDecimal32(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.DECIMAL32:
                return new Decimal32(ProtonStreamUtils.readInt(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Decimal32 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal64 readDecimal64(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.DECIMAL64:
                return new Decimal64(ProtonStreamUtils.readLong(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Decimal64 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Decimal128 readDecimal128(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.DECIMAL128:
                return new Decimal128(ProtonStreamUtils.readLong(stream), ProtonStreamUtils.readLong(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Decimal128 type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Short readShort(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.SHORT:
                return ProtonStreamUtils.readShort(stream);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public short readShort(InputStream stream, StreamDecoderState state, short defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.SHORT:
                return ProtonStreamUtils.readShort(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedShort readUnsignedShort(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return UnsignedShort.valueOf(ProtonStreamUtils.readShort(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Unsigned Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public short readUnsignedShort(InputStream stream, StreamDecoderState state, short defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return ProtonStreamUtils.readShort(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public int readUnsignedShort(InputStream stream, StreamDecoderState state, int defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return ProtonStreamUtils.readShort(stream) & 0xFFFF;
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Short type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Integer readInteger(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.SMALLINT:
                return (int) ProtonStreamUtils.readByte(stream);
            case EncodingCodes.INT:
                return ProtonStreamUtils.readInt(stream);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public int readInteger(InputStream stream, StreamDecoderState state, int defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.SMALLINT:
                return ProtonStreamUtils.readByte(stream);
            case EncodingCodes.INT:
                return ProtonStreamUtils.readInt(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedInteger readUnsignedInteger(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return UnsignedInteger.ZERO;
            case EncodingCodes.SMALLUINT:
                return UnsignedInteger.valueOf(ProtonStreamUtils.readByte(stream) & 0xff);
            case EncodingCodes.UINT:
                return UnsignedInteger.valueOf(ProtonStreamUtils.readInt(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Unsigned Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public int readUnsignedInteger(InputStream stream, StreamDecoderState state, int defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return 0;
            case EncodingCodes.SMALLUINT:
                return ProtonStreamUtils.readByte(stream) & 0xff;
            case EncodingCodes.UINT:
                return ProtonStreamUtils.readInt(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readUnsignedInteger(InputStream stream, StreamDecoderState state, long defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return 0;
            case EncodingCodes.SMALLUINT:
                return ProtonStreamUtils.readByte(stream) & 0xffl;
            case EncodingCodes.UINT:
                return ProtonStreamUtils.readInt(stream) & 0xffffffffl;
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Integer type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Long readLong(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.SMALLLONG:
                return Long.valueOf(ProtonStreamUtils.readByte(stream) & 0xffl);
            case EncodingCodes.LONG:
                return ProtonStreamUtils.readLong(stream);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readLong(InputStream stream, StreamDecoderState state, long defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.SMALLLONG:
                return ProtonStreamUtils.readByte(stream) & 0xffl;
            case EncodingCodes.LONG:
                return ProtonStreamUtils.readLong(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UnsignedLong readUnsignedLong(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.ULONG0:
                return UnsignedLong.ZERO;
            case EncodingCodes.SMALLULONG:
                return UnsignedLong.valueOf(ProtonStreamUtils.readByte(stream) & 0xffl);
            case EncodingCodes.ULONG:
                return UnsignedLong.valueOf(ProtonStreamUtils.readLong(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Unsigned Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readUnsignedLong(InputStream stream, StreamDecoderState state, long defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.ULONG0:
                return 0l;
            case EncodingCodes.SMALLULONG:
                return ProtonStreamUtils.readByte(stream) & 0xffl;
            case EncodingCodes.ULONG:
                return ProtonStreamUtils.readLong(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Unsigned Long type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Float readFloat(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.FLOAT:
                return Float.intBitsToFloat(ProtonStreamUtils.readInt(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Float type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public float readFloat(InputStream stream, StreamDecoderState state, float defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.FLOAT:
                return Float.intBitsToFloat(ProtonStreamUtils.readInt(stream));
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Float type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Double readDouble(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.DOUBLE:
                return Double.longBitsToDouble(ProtonStreamUtils.readLong(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Double type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public double readDouble(InputStream stream, StreamDecoderState state, double defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.DOUBLE:
                return Double.longBitsToDouble(ProtonStreamUtils.readLong(stream));
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Double type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Binary readBinary(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return binary8Decoder.readValue(stream, state);
            case EncodingCodes.VBIN32:
                return binary32Decoder.readValue(stream, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Binary type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public ProtonBuffer readBinaryAsBuffer(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return binary8Decoder.readValueAsBuffer(stream, state);
            case EncodingCodes.VBIN32:
                return binary32Decoder.readValueAsBuffer(stream, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Binary type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public DeliveryTag readDeliveryTag(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return new DeliveryTag.ProtonDeliveryTag(binary8Decoder.readValueAsArray(stream, state));
            case EncodingCodes.VBIN32:
                return new DeliveryTag.ProtonDeliveryTag(binary32Decoder.readValueAsArray(stream, state));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Binary type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public String readString(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.STR8:
                return string8Decoder.readValue(stream, state);
            case EncodingCodes.STR32:
                return string32Decoder.readValue(stream, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected String type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Symbol readSymbol(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.SYM8:
                return symbol8Decoder.readValue(stream, state);
            case EncodingCodes.SYM32:
                return symbol32Decoder.readValue(stream, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Symbol type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public String readSymbol(InputStream stream, StreamDecoderState state, String defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.SYM8:
                return symbol8Decoder.readString(stream, state);
            case EncodingCodes.SYM32:
                return symbol32Decoder.readString(stream, state);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Symbol type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public Long readTimestamp(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.TIMESTAMP:
                return ProtonStreamUtils.readLong(stream);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Timestamp type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public long readTimestamp(InputStream stream, StreamDecoderState state, long defaultValue) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.TIMESTAMP:
                return ProtonStreamUtils.readLong(stream);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new DecodeException("Expected Timestamp type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @Override
    public UUID readUUID(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.UUID:
                return new UUID(ProtonStreamUtils.readLong(stream), ProtonStreamUtils.readLong(stream));
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected UUID type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Map<K, V> readMap(InputStream stream, StreamDecoderState state) throws DecodeException {
         final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.MAP8:
                return (Map<K, V>) map8Decoder.readValue(stream, state);
            case EncodingCodes.MAP32:
                return (Map<K, V>) map32Decoder.readValue(stream, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected Map type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> List<V> readList(InputStream stream, StreamDecoderState state) throws DecodeException {
        final byte encodingCode = ProtonStreamUtils.readEncodingCode(stream);

        switch (encodingCode) {
            case EncodingCodes.LIST0:
                return Collections.emptyList();
            case EncodingCodes.LIST8:
                return (List<V>) list8Decoder.readValue(stream, state);
            case EncodingCodes.LIST32:
                return (List<V>) list32Decoder.readValue(stream, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new DecodeException("Expected List type but found encoding: " + EncodingCodes.toString(encodingCode));
        }
    }

    private ClassCastException signalUnexpectedType(final Object val, Class<?> clazz) {
        return new ClassCastException("Unexpected type " + val.getClass().getName() +
                                      ". Expected " + clazz.getName() + ".");
    }

    private StreamTypeDecoder<?> handleUnknownDescribedType(final Object descriptor) {
        StreamTypeDecoder<?> StreamTypeDecoder = new UnknownDescribedTypeDecoder() {

            @Override
            public Object getDescriptor() {
                return descriptor;
            }
        };

        describedTypeDecoders.put(descriptor, (UnknownDescribedTypeDecoder) StreamTypeDecoder);

        return StreamTypeDecoder;
    }
}
