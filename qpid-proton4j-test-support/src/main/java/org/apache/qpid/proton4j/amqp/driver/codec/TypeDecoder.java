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
package org.apache.qpid.proton4j.amqp.driver.codec;

import java.nio.charset.Charset;
import java.util.Date;
import java.util.UUID;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Decimal128;
import org.apache.qpid.proton4j.types.Decimal32;
import org.apache.qpid.proton4j.types.Decimal64;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedByte;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.UnsignedShort;

class TypeDecoder {

    private static final Charset ASCII = Charset.forName("US-ASCII");
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final TypeConstructor[] constructors = new TypeConstructor[256];

    static {
        constructors[0x00] = new DescribedTypeConstructor();

        constructors[0x40] = new NullConstructor();
        constructors[0x41] = new TrueConstructor();
        constructors[0x42] = new FalseConstructor();
        constructors[0x43] = new UInt0Constructor();
        constructors[0x44] = new ULong0Constructor();
        constructors[0x45] = new EmptyListConstructor();

        constructors[0x50] = new UByteConstructor();
        constructors[0x51] = new ByteConstructor();
        constructors[0x52] = new SmallUIntConstructor();
        constructors[0x53] = new SmallULongConstructor();
        constructors[0x54] = new SmallIntConstructor();
        constructors[0x55] = new SmallLongConstructor();
        constructors[0x56] = new BooleanConstructor();

        constructors[0x60] = new UShortConstructor();
        constructors[0x61] = new ShortConstructor();

        constructors[0x70] = new UIntConstructor();
        constructors[0x71] = new IntConstructor();
        constructors[0x72] = new FloatConstructor();
        constructors[0x73] = new CharConstructor();
        constructors[0x74] = new Decimal32Constructor();

        constructors[0x80] = new ULongConstructor();
        constructors[0x81] = new LongConstructor();
        constructors[0x82] = new DoubleConstructor();
        constructors[0x83] = new TimestampConstructor();
        constructors[0x84] = new Decimal64Constructor();

        constructors[0x94] = new Decimal128Constructor();
        constructors[0x98] = new UUIDConstructor();

        constructors[0xa0] = new SmallBinaryConstructor();
        constructors[0xa1] = new SmallStringConstructor();
        constructors[0xa3] = new SmallSymbolConstructor();

        constructors[0xb0] = new BinaryConstructor();
        constructors[0xb1] = new StringConstructor();
        constructors[0xb3] = new SymbolConstructor();

        constructors[0xc0] = new SmallListConstructor();
        constructors[0xc1] = new SmallMapConstructor();

        constructors[0xd0] = new ListConstructor();
        constructors[0xd1] = new MapConstructor();

        constructors[0xe0] = new SmallArrayConstructor();
        constructors[0xf0] = new ArrayConstructor();
    }

    private interface TypeConstructor {

        Codec.DataType getType();

        int size(ProtonBuffer buffer);

        void parse(ProtonBuffer buffer, Codec data);
    }

    static int decode(ProtonBuffer buffer, Codec data) {
        if (buffer.isReadable()) {
            int position = buffer.getReadIndex();
            TypeConstructor c = readConstructor(buffer);
            final int size = c.size(buffer);
            if (buffer.getReadableBytes() >= size) {
                c.parse(buffer, data);
                return 1 + size;
            } else {
                buffer.setReadIndex(position);
                return -4;
            }
        }
        return 0;
    }

    private static TypeConstructor readConstructor(ProtonBuffer buffer) {
        int index = buffer.readByte() & 0xff;
        TypeConstructor tc = constructors[index];
        if (tc == null) {
            throw new IllegalArgumentException("No constructor for type " + index);
        }
        return tc;
    }

    private static class NullConstructor implements TypeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.NULL;
        }

        @Override
        public int size(ProtonBuffer buffer) {
            return 0;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putNull();
        }
    }

    private static class TrueConstructor implements TypeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.BOOL;
        }

        @Override
        public int size(ProtonBuffer buffer) {
            return 0;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putBoolean(true);
        }
    }

    private static class FalseConstructor implements TypeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.BOOL;
        }

        @Override
        public int size(ProtonBuffer buffer) {
            return 0;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putBoolean(false);
        }
    }

    private static class UInt0Constructor implements TypeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.UINT;
        }

        @Override
        public int size(ProtonBuffer buffer) {
            return 0;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUnsignedInteger(UnsignedInteger.ZERO);
        }
    }

    private static class ULong0Constructor implements TypeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.ULONG;
        }

        @Override
        public int size(ProtonBuffer buffer) {
            return 0;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUnsignedLong(UnsignedLong.ZERO);
        }
    }

    private static class EmptyListConstructor implements TypeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.LIST;
        }

        @Override
        public int size(ProtonBuffer buffer) {
            return 0;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putList();
        }
    }

    @SuppressWarnings("unused")
    private static abstract class Fixed0SizeConstructor implements TypeConstructor {

        @Override
        public final int size(ProtonBuffer buffer) {
            return 0;
        }
    }

    private static abstract class Fixed1SizeConstructor implements TypeConstructor {

        @Override
        public int size(ProtonBuffer buffer) {
            return 1;
        }
    }

    private static abstract class Fixed2SizeConstructor implements TypeConstructor {

        @Override
        public int size(ProtonBuffer buffer) {
            return 2;
        }
    }

    private static abstract class Fixed4SizeConstructor implements TypeConstructor {

        @Override
        public int size(ProtonBuffer buffer) {
            return 4;
        }
    }

    private static abstract class Fixed8SizeConstructor implements TypeConstructor {

        @Override
        public int size(ProtonBuffer buffer) {
            return 8;
        }
    }

    private static abstract class Fixed16SizeConstructor implements TypeConstructor {

        @Override
        public int size(ProtonBuffer buffer) {
            return 16;
        }
    }

    private static class UByteConstructor extends Fixed1SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.UBYTE;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUnsignedByte(UnsignedByte.valueOf(buffer.readByte()));
        }
    }

    private static class ByteConstructor extends Fixed1SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.BYTE;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putByte(buffer.readByte());
        }
    }

    private static class SmallUIntConstructor extends Fixed1SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.UINT;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUnsignedInteger(UnsignedInteger.valueOf((buffer.readByte()) & 0xff));
        }
    }

    private static class SmallIntConstructor extends Fixed1SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.INT;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putInt(buffer.readByte());
        }
    }

    private static class SmallULongConstructor extends Fixed1SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.ULONG;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUnsignedLong(UnsignedLong.valueOf((buffer.readByte()) & 0xff));
        }
    }

    private static class SmallLongConstructor extends Fixed1SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.LONG;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putLong(buffer.readByte());
        }
    }

    private static class BooleanConstructor extends Fixed1SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.BOOL;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int i = buffer.readByte();
            if (i != 0 && i != 1) {
                throw new IllegalArgumentException("Illegal value " + i + " for boolean");
            }
            data.putBoolean(i == 1);
        }
    }

    private static class UShortConstructor extends Fixed2SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.USHORT;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUnsignedShort(UnsignedShort.valueOf(buffer.readShort()));
        }
    }

    private static class ShortConstructor extends Fixed2SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.SHORT;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putShort(buffer.readShort());
        }
    }

    private static class UIntConstructor extends Fixed4SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.UINT;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUnsignedInteger(UnsignedInteger.valueOf(buffer.readInt()));
        }
    }

    private static class IntConstructor extends Fixed4SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.INT;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putInt(buffer.readInt());
        }
    }

    private static class FloatConstructor extends Fixed4SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.FLOAT;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putFloat(buffer.readFloat());
        }
    }

    private static class CharConstructor extends Fixed4SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.CHAR;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putChar(buffer.readInt());
        }
    }

    private static class Decimal32Constructor extends Fixed4SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.DECIMAL32;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putDecimal32(new Decimal32(buffer.readInt()));
        }
    }

    private static class ULongConstructor extends Fixed8SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.ULONG;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUnsignedLong(UnsignedLong.valueOf(buffer.readLong()));
        }
    }

    private static class LongConstructor extends Fixed8SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.LONG;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putLong(buffer.readLong());
        }
    }

    private static class DoubleConstructor extends Fixed8SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.DOUBLE;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putDouble(buffer.readDouble());
        }
    }

    private static class TimestampConstructor extends Fixed8SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.TIMESTAMP;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putTimestamp(new Date(buffer.readLong()));
        }
    }

    private static class Decimal64Constructor extends Fixed8SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.DECIMAL64;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putDecimal64(new Decimal64(buffer.readLong()));
        }
    }

    private static class Decimal128Constructor extends Fixed16SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.DECIMAL128;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putDecimal128(new Decimal128(buffer.readLong(), buffer.readLong()));
        }
    }

    private static class UUIDConstructor extends Fixed16SizeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.UUID;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putUUID(new UUID(buffer.readLong(), buffer.readLong()));
        }
    }

    private static abstract class SmallVariableConstructor implements TypeConstructor {

        @Override
        public int size(ProtonBuffer buffer) {
            int position = buffer.getReadIndex();
            if (buffer.isReadable()) {
                int size = buffer.readByte() & 0xff;
                buffer.setReadIndex(position);

                return size + 1;
            } else {
                return 1;
            }
        }

    }

    private static abstract class VariableConstructor implements TypeConstructor {

        @Override
        public int size(ProtonBuffer buffer) {
            int position = buffer.getReadIndex();
            if (buffer.getReadableBytes() >= 4) {
                int size = buffer.readInt();
                buffer.setReadIndex(position);

                return size + 4;
            } else {
                return 4;
            }
        }

    }

    private static class SmallBinaryConstructor extends SmallVariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.BINARY;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readByte() & 0xff;
            byte[] bytes = new byte[size];
            buffer.readBytes(bytes);
            data.putBinary(bytes);
        }
    }

    private static class SmallSymbolConstructor extends SmallVariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.SYMBOL;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readByte() & 0xff;
            byte[] bytes = new byte[size];
            buffer.readBytes(bytes);
            data.putSymbol(Symbol.valueOf(new String(bytes, ASCII)));
        }
    }

    private static class SmallStringConstructor extends SmallVariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.STRING;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readByte() & 0xff;
            byte[] bytes = new byte[size];
            buffer.readBytes(bytes);
            data.putString(new String(bytes, UTF_8));
        }
    }

    private static class BinaryConstructor extends VariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.BINARY;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readInt();
            byte[] bytes = new byte[size];
            buffer.readBytes(bytes);
            data.putBinary(bytes);
        }
    }

    private static class SymbolConstructor extends VariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.SYMBOL;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readInt();
            byte[] bytes = new byte[size];
            buffer.readBytes(bytes);
            data.putSymbol(Symbol.valueOf(new String(bytes, ASCII)));
        }
    }

    private static class StringConstructor extends VariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.STRING;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readInt();
            byte[] bytes = new byte[size];
            buffer.readBytes(bytes);
            data.putString(new String(bytes, UTF_8));
        }
    }

    private static class SmallListConstructor extends SmallVariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.LIST;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readByte() & 0xff;
            ProtonBuffer buf = buffer.slice(buffer.getReadIndex(), size);
            buffer.skipBytes(size);
            int count = buf.readByte() & 0xff;
            data.putList();
            parseChildren(data, buf, count);
        }
    }

    private static class SmallMapConstructor extends SmallVariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.MAP;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readByte() & 0xff;
            ProtonBuffer buf = buffer.slice(buffer.getReadIndex(), size);
            buffer.skipBytes(size);
            int count = buf.readByte() & 0xff;
            data.putMap();
            parseChildren(data, buf, count);
        }
    }

    private static class ListConstructor extends VariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.LIST;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readInt();
            ProtonBuffer buf = buffer.slice(buffer.getReadIndex(), size);
            buffer.skipBytes(size);
            int count = buf.readInt();
            data.putList();
            parseChildren(data, buf, count);
        }
    }

    private static class MapConstructor extends VariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.MAP;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            int size = buffer.readInt();
            ProtonBuffer buf = buffer.slice(buffer.getReadIndex(), size);
            buffer.skipBytes(size);
            int count = buf.readInt();
            data.putMap();
            parseChildren(data, buf, count);
        }
    }

    private static void parseChildren(Codec data, ProtonBuffer buf, int count) {
        data.enter();
        for (int i = 0; i < count; i++) {
            TypeConstructor c = readConstructor(buf);
            final int size = c.size(buf);
            final int getReadableBytes = buf.getReadableBytes();
            if (size <= getReadableBytes) {
                c.parse(buf, data);
            } else {
                throw new IllegalArgumentException("Malformed data");
            }

        }
        data.exit();
    }

    private static class DescribedTypeConstructor implements TypeConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.DESCRIBED;
        }

        @Override
        public int size(ProtonBuffer buffer) {
            ProtonBuffer buf = buffer.slice();
            if (buf.isReadable()) {
                TypeConstructor c = readConstructor(buf);
                int size = c.size(buf);
                if (buf.getReadableBytes() > size) {
                    buf.setReadIndex(size + 1);
                    c = readConstructor(buf);
                    return size + 2 + c.size(buf);
                } else {
                    return size + 2;
                }
            } else {
                return 1;
            }
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {
            data.putDescribed();
            data.enter();
            TypeConstructor c = readConstructor(buffer);
            c.parse(buffer, data);
            c = readConstructor(buffer);
            c.parse(buffer, data);
            data.exit();
        }
    }

    private static class SmallArrayConstructor extends SmallVariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.ARRAY;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {

            int size = buffer.readByte() & 0xff;
            ProtonBuffer buf = buffer.slice(buffer.getReadIndex(), size);
            buffer.skipBytes(size);
            int count = buf.readByte() & 0xff;
            parseArray(data, buf, count);
        }
    }

    private static class ArrayConstructor extends VariableConstructor {

        @Override
        public Codec.DataType getType() {
            return Codec.DataType.ARRAY;
        }

        @Override
        public void parse(ProtonBuffer buffer, Codec data) {

            int size = buffer.readInt();
            ProtonBuffer buf = buffer.slice(buffer.getReadIndex(), size);
            buffer.skipBytes(size);
            int count = buf.readInt();
            parseArray(data, buf, count);
        }
    }

    private static void parseArray(Codec data, ProtonBuffer buffer, int count) {
        byte type = buffer.readByte();
        boolean isDescribed = type == (byte) 0x00;
        int descriptorPosition = buffer.getReadIndex();
        if (isDescribed) {
            TypeConstructor descriptorTc = readConstructor(buffer);
            buffer.skipBytes(descriptorTc.size(buffer));
            type = buffer.readByte();
            if (type == (byte) 0x00) {
                throw new IllegalArgumentException("Malformed array data");
            }

        }
        TypeConstructor tc = constructors[type & 0xff];

        data.putArray(isDescribed, tc.getType());
        data.enter();
        if (isDescribed) {
            int position = buffer.getReadIndex();
            buffer.setReadIndex(descriptorPosition);
            TypeConstructor descriptorTc = readConstructor(buffer);
            descriptorTc.parse(buffer, data);
            buffer.setReadIndex(position);
        }
        for (int i = 0; i < count; i++) {
            tc.parse(buffer, data);
        }

        data.exit();
    }
}