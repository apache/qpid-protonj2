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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Decimal128;
import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.Decoder;
import org.apache.qpid.proton4j.codec.DecoderState;
import org.apache.qpid.proton4j.codec.DescribedTypeDecoder;
import org.apache.qpid.proton4j.codec.EncodingCodes;
import org.apache.qpid.proton4j.codec.PrimitiveArrayTypeDecoder;
import org.apache.qpid.proton4j.codec.PrimitiveTypeDecoder;
import org.apache.qpid.proton4j.codec.TypeDecoder;

/**
 * The default AMQP Decoder implementation.
 */
public class ProtonDecoder implements Decoder {

    // The decoders for primitives are fixed and cannot be altered by users who want
    // to register custom decoders.
    private PrimitiveTypeDecoder<?>[] primitiveDecoders = new PrimitiveTypeDecoder[256];

    // Registry of decoders for described types which can be updated with user defined
    // decoders as well as the default decoders.
    private Map<Object, DescribedTypeDecoder<?>> describedTypeDecoders = new HashMap<>();

    @Override
    public DecoderState newDecoderState() {
        return new ProtonDecoderState(this);
    }

    @Override
    public Object readObject(ProtonBuffer buffer, DecoderState state) throws IOException {
        TypeDecoder<?> decoder = readNextTypeDecoder(buffer, state);

        if (decoder == null) {
            throw new IOException("Unknown type constructor in encoded bytes");
        }

        if (decoder instanceof PrimitiveArrayTypeDecoder) {
            PrimitiveArrayTypeDecoder arrayDecoder = (PrimitiveArrayTypeDecoder) decoder;
            return arrayDecoder.readValueAsObject(buffer, state);
        } else {
            return decoder.readValue(buffer, state);
        }
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
            throw signalUnexpectedType(result, Array.newInstance(clazz, 0).getClass());
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
        int encodingCode = buffer.readByte() & 0xff;

        if (encodingCode == EncodingCodes.DESCRIBED_TYPE_INDICATOR) {
            byte encoding = buffer.getByte(buffer.getReadIndex());

            final Object descriptor;

            switch (encoding) {
                case EncodingCodes.SMALLULONG:
                case EncodingCodes.ULONG:
                    descriptor = readUnsignedLong(buffer, state);
                    break;
                case EncodingCodes.SYM8:
                case EncodingCodes.SYM32:
                    descriptor = readSymbol(buffer, state);
                    break;
                default:
                    descriptor = readObject(buffer, state);
            }

            TypeDecoder<?> typeDecoder = describedTypeDecoders.get(descriptor);
            if (typeDecoder == null) {
                typeDecoder = handleUnknownDescribedType(descriptor);
            }

            return typeDecoder;
        } else {
            if (encodingCode > primitiveDecoders.length) {
                throw new IOException("Read unknown encoding code from buffer");
            }

            return primitiveDecoders[encodingCode];
        }
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
        describedTypeDecoders.put(decoder.getDescriptorCode(), decoder);
        describedTypeDecoders.put(decoder.getDescriptorSymbol(), decoder);
        return this;
    }

    @Override
    public <V> ProtonDecoder registerPrimitiveTypeDecoder(PrimitiveTypeDecoder<V> decoder) {
        primitiveDecoders[decoder.getTypeCode()] = decoder;
        return this;
    }

    @Override
    public TypeDecoder<?> getTypeDecoder(Object instance) {
        return null;
    }

    @Override
    public Boolean readBoolean(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BOOLEAN_TRUE:
                return (Boolean) primitiveDecoders[EncodingCodes.BOOLEAN_TRUE & 0xff].readValue(buffer, state);
            case EncodingCodes.BOOLEAN_FALSE:
                return (Boolean) primitiveDecoders[EncodingCodes.BOOLEAN_FALSE & 0xff].readValue(buffer, state);
            case EncodingCodes.BOOLEAN:
                return (Boolean) primitiveDecoders[EncodingCodes.BOOLEAN & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected boolean type but found encoding: " + encodingCode);
        }
    }

    @Override
    public boolean readBoolean(ProtonBuffer buffer, DecoderState state, boolean defaultValue) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BOOLEAN_TRUE:
                return (Boolean) primitiveDecoders[EncodingCodes.BOOLEAN_TRUE & 0xff].readValue(buffer, state);
            case EncodingCodes.BOOLEAN_FALSE:
                return (Boolean) primitiveDecoders[EncodingCodes.BOOLEAN_FALSE & 0xff].readValue(buffer, state);
            case EncodingCodes.BOOLEAN:
                return (Boolean) primitiveDecoders[EncodingCodes.BOOLEAN & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return defaultValue;
            default:
                throw new IOException("Expected boolean type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Byte readByte(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.BYTE:
                return (Byte) primitiveDecoders[EncodingCodes.BYTE & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected byte type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UnsignedByte readUnsignedByte(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UBYTE:
                return (UnsignedByte) primitiveDecoders[EncodingCodes.UBYTE & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected unsigned byte type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Character readCharacter(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.CHAR:
                return (Character) primitiveDecoders[EncodingCodes.CHAR & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Character type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Decimal32 readDecimal32(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL32:
                return (Decimal32) primitiveDecoders[EncodingCodes.DECIMAL32 & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal32 type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Decimal64 readDecimal64(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL64:
                return (Decimal64) primitiveDecoders[EncodingCodes.DECIMAL64 & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal64 type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Decimal128 readDecimal128(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DECIMAL128:
                return (Decimal128) primitiveDecoders[EncodingCodes.DECIMAL128 & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Decimal128 type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Short readShort(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SHORT:
                return (Short) primitiveDecoders[EncodingCodes.SHORT & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Short type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UnsignedShort readUnsignedShort(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.USHORT:
                return (UnsignedShort) primitiveDecoders[EncodingCodes.USHORT & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UnsignedShort type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Integer readInteger(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SMALLINT:
                return (Integer) primitiveDecoders[EncodingCodes.SMALLINT & 0xff].readValue(buffer, state);
            case EncodingCodes.INT:
                return (Integer) primitiveDecoders[EncodingCodes.INT & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Integer type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UnsignedInteger readUnsignedInteger(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UINT0:
                return (UnsignedInteger) primitiveDecoders[EncodingCodes.UINT0 & 0xff].readValue(buffer, state);
            case EncodingCodes.SMALLUINT:
                return (UnsignedInteger) primitiveDecoders[EncodingCodes.SMALLUINT & 0xff].readValue(buffer, state);
            case EncodingCodes.UINT:
                return (UnsignedInteger) primitiveDecoders[EncodingCodes.UINT & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UnsignedInteger type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Long readLong(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SMALLLONG:
                return (Long) primitiveDecoders[EncodingCodes.SMALLLONG & 0xff].readValue(buffer, state);
            case EncodingCodes.LONG:
                return (Long) primitiveDecoders[EncodingCodes.LONG & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Long type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UnsignedLong readUnsignedLong(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.ULONG0:
                return (UnsignedLong) primitiveDecoders[EncodingCodes.ULONG0 & 0xff].readValue(buffer, state);
            case EncodingCodes.SMALLULONG:
                return (UnsignedLong) primitiveDecoders[EncodingCodes.SMALLULONG & 0xff].readValue(buffer, state);
            case EncodingCodes.ULONG:
                return (UnsignedLong) primitiveDecoders[EncodingCodes.ULONG & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Long type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Float readFloat(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.FLOAT:
                return (Float) primitiveDecoders[EncodingCodes.FLOAT & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Float type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Double readDouble(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.DOUBLE:
                return (Double) primitiveDecoders[EncodingCodes.DOUBLE & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Double type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Binary readBinary(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.VBIN8:
                return (Binary) primitiveDecoders[EncodingCodes.VBIN8 & 0xff].readValue(buffer, state);
            case EncodingCodes.VBIN32:
                return (Binary) primitiveDecoders[EncodingCodes.VBIN32 & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Binary type but found encoding: " + encodingCode);
        }
    }

    @Override
    public String readString(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.STR8:
                return (String) primitiveDecoders[EncodingCodes.STR8 & 0xff].readValue(buffer, state);
            case EncodingCodes.STR32:
                return (String) primitiveDecoders[EncodingCodes.STR32 & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected String type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Symbol readSymbol(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.SYM8:
                return (Symbol) primitiveDecoders[EncodingCodes.SYM8 & 0xff].readValue(buffer, state);
            case EncodingCodes.SYM32:
                return (Symbol) primitiveDecoders[EncodingCodes.SYM32 & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Symbol type but found encoding: " + encodingCode);
        }
    }

    @Override
    public Long readTimestamp(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.TIMESTAMP:
                return (Long) primitiveDecoders[EncodingCodes.TIMESTAMP & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Timestamp type but found encoding: " + encodingCode);
        }
    }

    @Override
    public UUID readUUID(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.UUID:
                return (UUID) primitiveDecoders[EncodingCodes.UUID & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected UUID type but found encoding: " + encodingCode);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> Map<K, V> readMap(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.MAP8:
                return (Map<K, V>) primitiveDecoders[EncodingCodes.MAP8 & 0xff].readValue(buffer, state);
            case EncodingCodes.MAP32:
                return (Map<K, V>) primitiveDecoders[EncodingCodes.MAP32 & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected Map type but found encoding: " + encodingCode);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> List<V> readList(ProtonBuffer buffer, DecoderState state) throws IOException {
        byte encodingCode = buffer.readByte();

        switch (encodingCode) {
            case EncodingCodes.LIST0:
                return (List<V>) primitiveDecoders[EncodingCodes.LIST0 & 0xff].readValue(buffer, state);
            case EncodingCodes.LIST8:
                return (List<V>) primitiveDecoders[EncodingCodes.LIST8 & 0xff].readValue(buffer, state);
            case EncodingCodes.LIST32:
                return (List<V>) primitiveDecoders[EncodingCodes.LIST32 & 0xff].readValue(buffer, state);
            case EncodingCodes.NULL:
                return null;
            default:
                throw new IOException("Expected List type but found encoding: " + encodingCode);
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
