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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.types.Binary;
import org.apache.qpid.proton4j.types.Decimal128;
import org.apache.qpid.proton4j.types.Decimal32;
import org.apache.qpid.proton4j.types.Decimal64;
import org.apache.qpid.proton4j.types.DescribedType;
import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedByte;
import org.apache.qpid.proton4j.types.UnsignedInteger;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.UnsignedShort;

public class CodecImpl implements Codec {

    private Element<?> first;
    private Element<?> current;
    private Element<?> parent;

    public CodecImpl() {
    }

    @Override
    public void free() {
        first = null;
        current = null;
    }

    @Override
    public void clear() {
        first = null;
        current = null;
        parent = null;
    }

    @Override
    public long size() {
        return first == null ? 0 : first.size();
    }

    @Override
    public void rewind() {
        current = null;
        parent = null;
    }

    @Override
    public DataType next() {
        Element<?> next = current == null ? (parent == null ? first : parent.child()) : current.next();

        if (next != null) {
            current = next;
        }
        return next == null ? null : next.getDataType();
    }

    @Override
    public DataType prev() {
        Element<?> prev = current == null ? null : current.prev();

        current = prev;
        return prev == null ? null : prev.getDataType();
    }

    @Override
    public boolean enter() {
        if (current != null && current.canEnter()) {
            parent = current;
            current = null;
            return true;
        }
        return false;
    }

    @Override
    public boolean exit() {
        if (parent != null) {
            Element<?> oldParent = this.parent;
            current = oldParent;
            parent = current.parent();
            return true;

        }
        return false;
    }

    @Override
    public DataType type() {
        return current == null ? null : current.getDataType();
    }

    @Override
    public long encodedSize() {
        int size = 0;
        Element<?> elt = first;
        while (elt != null) {
            size += elt.size();
            elt = elt.next();
        }
        return size;
    }

    @Override
    public long encode(ProtonBuffer buffer) {
        Element<?> elt = first;
        int size = 0;
        while (elt != null) {
            final int eltSize = elt.size();
            if (eltSize <= buffer.getMaxWritableBytes()) {
                size += elt.encode(buffer);
            } else {
                size += eltSize;
            }
            elt = elt.next();
        }
        return size;
    }

    @Override
    public long decode(ProtonBuffer buffer) {
        return TypeDecoder.decode(buffer, this);
    }

    private void putElement(Element<?> element) {
        if (first == null) {
            first = element;
        } else {
            if (current == null) {
                if (parent == null) {
                    first = first.replaceWith(element);
                    element = first;
                } else {
                    element = parent.addChild(element);
                }
            } else {
                if (parent != null) {
                    element = parent.checkChild(element);
                }
                current.setNext(element);
            }
        }

        current = element;
    }

    @Override
    public void putList() {
        putElement(new ListElement(parent, current));
    }

    @Override
    public void putMap() {
        putElement(new MapElement(parent, current));
    }

    @Override
    public void putArray(boolean described, DataType type) {
        putElement(new ArrayElement(parent, current, described, type));
    }

    @Override
    public void putDescribed() {
        putElement(new DescribedTypeElement(parent, current));
    }

    @Override
    public void putNull() {
        putElement(new NullElement(parent, current));
    }

    @Override
    public void putBoolean(boolean b) {
        putElement(new BooleanElement(parent, current, b));
    }

    @Override
    public void putUnsignedByte(UnsignedByte ub) {
        putElement(new UnsignedByteElement(parent, current, ub));
    }

    @Override
    public void putByte(byte b) {
        putElement(new ByteElement(parent, current, b));
    }

    @Override
    public void putUnsignedShort(UnsignedShort us) {
        putElement(new UnsignedShortElement(parent, current, us));
    }

    @Override
    public void putShort(short s) {
        putElement(new ShortElement(parent, current, s));
    }

    @Override
    public void putUnsignedInteger(UnsignedInteger ui) {
        putElement(new UnsignedIntegerElement(parent, current, ui));
    }

    @Override
    public void putInt(int i) {
        putElement(new IntegerElement(parent, current, i));
    }

    @Override
    public void putChar(int c) {
        putElement(new CharElement(parent, current, c));
    }

    @Override
    public void putUnsignedLong(UnsignedLong ul) {
        putElement(new UnsignedLongElement(parent, current, ul));
    }

    @Override
    public void putLong(long l) {
        putElement(new LongElement(parent, current, l));
    }

    @Override
    public void putTimestamp(Date t) {
        putElement(new TimestampElement(parent, current, t));
    }

    @Override
    public void putFloat(float f) {
        putElement(new FloatElement(parent, current, f));
    }

    @Override
    public void putDouble(double d) {
        putElement(new DoubleElement(parent, current, d));
    }

    @Override
    public void putDecimal32(Decimal32 d) {
        putElement(new Decimal32Element(parent, current, d));
    }

    @Override
    public void putDecimal64(Decimal64 d) {
        putElement(new Decimal64Element(parent, current, d));
    }

    @Override
    public void putDecimal128(Decimal128 d) {
        putElement(new Decimal128Element(parent, current, d));
    }

    @Override
    public void putUUID(UUID u) {
        putElement(new UUIDElement(parent, current, u));
    }

    @Override
    public void putBinary(Binary bytes) {
        putElement(new BinaryElement(parent, current, bytes));
    }

    @Override
    public void putBinary(byte[] bytes) {
        putBinary(new Binary(bytes));
    }

    @Override
    public void putString(String string) {
        putElement(new StringElement(parent, current, string));
    }

    @Override
    public void putSymbol(Symbol symbol) {
        putElement(new SymbolElement(parent, current, symbol));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putObject(Object o) {
        if (o == null) {
            putNull();
        } else if (o instanceof Boolean) {
            putBoolean((Boolean) o);
        } else if (o instanceof UnsignedByte) {
            putUnsignedByte((UnsignedByte) o);
        } else if (o instanceof Byte) {
            putByte((Byte) o);
        } else if (o instanceof UnsignedShort) {
            putUnsignedShort((UnsignedShort) o);
        } else if (o instanceof Short) {
            putShort((Short) o);
        } else if (o instanceof UnsignedInteger) {
            putUnsignedInteger((UnsignedInteger) o);
        } else if (o instanceof Integer) {
            putInt((Integer) o);
        } else if (o instanceof Character) {
            putChar((Character) o);
        } else if (o instanceof UnsignedLong) {
            putUnsignedLong((UnsignedLong) o);
        } else if (o instanceof Long) {
            putLong((Long) o);
        } else if (o instanceof Date) {
            putTimestamp((Date) o);
        } else if (o instanceof Float) {
            putFloat((Float) o);
        } else if (o instanceof Double) {
            putDouble((Double) o);
        } else if (o instanceof Decimal32) {
            putDecimal32((Decimal32) o);
        } else if (o instanceof Decimal64) {
            putDecimal64((Decimal64) o);
        } else if (o instanceof Decimal128) {
            putDecimal128((Decimal128) o);
        } else if (o instanceof UUID) {
            putUUID((UUID) o);
        } else if (o instanceof Binary) {
            putBinary((Binary) o);
        } else if (o instanceof String) {
            putString((String) o);
        } else if (o instanceof Symbol) {
            putSymbol((Symbol) o);
        } else if (o instanceof DescribedType) {
            putDescribedType((DescribedType) o);
        } else if (o instanceof Symbol[]) {
            putArray(false, Codec.DataType.SYMBOL);
            enter();
            for (Symbol s : (Symbol[]) o) {
                putSymbol(s);
            }
            exit();
        } else if (o instanceof Object[]) {
            throw new IllegalArgumentException("Unsupported array type");
        } else if (o instanceof List) {
            putJavaList((List<Object>) o);
        } else if (o instanceof Map) {
            putJavaMap((Map<Object, Object>) o);
        } else {
            throw new IllegalArgumentException("Unknown type " + o.getClass().getSimpleName());
        }
    }

    @Override
    public void putJavaMap(Map<Object, Object> map) {
        putMap();
        enter();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            putObject(entry.getKey());
            putObject(entry.getValue());
        }
        exit();
    }

    @Override
    public void putJavaList(List<Object> list) {
        putList();
        enter();
        for (Object o : list) {
            putObject(o);
        }
        exit();
    }

    @Override
    public void putDescribedType(DescribedType dt) {
        putElement(new DescribedTypeElement(parent, current));
        enter();
        putObject(dt.getDescriptor());
        putObject(dt.getDescribed());
        exit();
    }

    @Override
    public long getList() {
        if (current instanceof ListElement) {
            return ((ListElement) current).count();
        }
        throw new IllegalStateException("Current value not list");
    }

    @Override
    public long getMap() {
        if (current instanceof MapElement) {
            return ((MapElement) current).count();
        }
        throw new IllegalStateException("Current value not map");
    }

    @Override
    public long getArray() {
        if (current instanceof ArrayElement) {
            return ((ArrayElement) current).count();
        }
        throw new IllegalStateException("Current value not array");
    }

    @Override
    public boolean isArrayDescribed() {
        if (current instanceof ArrayElement) {
            return ((ArrayElement) current).isDescribed();
        }
        throw new IllegalStateException("Current value not array");
    }

    @Override
    public DataType getArrayType() {
        if (current instanceof ArrayElement) {
            return ((ArrayElement) current).getArrayDataType();
        }
        throw new IllegalStateException("Current value not array");
    }

    @Override
    public boolean isDescribed() {
        return current != null && current.getDataType() == DataType.DESCRIBED;
    }

    @Override
    public boolean isNull() {
        return current != null && current.getDataType() == DataType.NULL;
    }

    @Override
    public boolean getBoolean() {
        if (current instanceof BooleanElement) {
            return ((BooleanElement) current).getValue();
        }
        throw new IllegalStateException("Current value not boolean");
    }

    @Override
    public UnsignedByte getUnsignedByte() {
        if (current instanceof UnsignedByteElement) {
            return ((UnsignedByteElement) current).getValue();
        }
        throw new IllegalStateException("Current value not unsigned byte");
    }

    @Override
    public byte getByte() {
        if (current instanceof ByteElement) {
            return ((ByteElement) current).getValue();
        }
        throw new IllegalStateException("Current value not byte");
    }

    @Override
    public UnsignedShort getUnsignedShort() {
        if (current instanceof UnsignedShortElement) {
            return ((UnsignedShortElement) current).getValue();
        }
        throw new IllegalStateException("Current value not unsigned short");
    }

    @Override
    public short getShort() {
        if (current instanceof ShortElement) {
            return ((ShortElement) current).getValue();
        }
        throw new IllegalStateException("Current value not short");
    }

    @Override
    public UnsignedInteger getUnsignedInteger() {
        if (current instanceof UnsignedIntegerElement) {
            return ((UnsignedIntegerElement) current).getValue();
        }
        throw new IllegalStateException("Current value not unsigned integer");
    }

    @Override
    public int getInt() {
        if (current instanceof IntegerElement) {
            return ((IntegerElement) current).getValue();
        }
        throw new IllegalStateException("Current value not integer");
    }

    @Override
    public int getChar() {
        if (current instanceof CharElement) {
            return ((CharElement) current).getValue();
        }
        throw new IllegalStateException("Current value not char");
    }

    @Override
    public UnsignedLong getUnsignedLong() {
        if (current instanceof UnsignedLongElement) {
            return ((UnsignedLongElement) current).getValue();
        }
        throw new IllegalStateException("Current value not unsigned long");
    }

    @Override
    public long getLong() {
        if (current instanceof LongElement) {
            return ((LongElement) current).getValue();
        }
        throw new IllegalStateException("Current value not long");
    }

    @Override
    public Date getTimestamp() {
        if (current instanceof TimestampElement) {
            return ((TimestampElement) current).getValue();
        }
        throw new IllegalStateException("Current value not timestamp");
    }

    @Override
    public float getFloat() {
        if (current instanceof FloatElement) {
            return ((FloatElement) current).getValue();
        }
        throw new IllegalStateException("Current value not float");
    }

    @Override
    public double getDouble() {
        if (current instanceof DoubleElement) {
            return ((DoubleElement) current).getValue();
        }
        throw new IllegalStateException("Current value not double");
    }

    @Override
    public Decimal32 getDecimal32() {
        if (current instanceof Decimal32Element) {
            return ((Decimal32Element) current).getValue();
        }
        throw new IllegalStateException("Current value not decimal32");
    }

    @Override
    public Decimal64 getDecimal64() {
        if (current instanceof Decimal64Element) {
            return ((Decimal64Element) current).getValue();
        }
        throw new IllegalStateException("Current value not decimal32");
    }

    @Override
    public Decimal128 getDecimal128() {
        if (current instanceof Decimal128Element) {
            return ((Decimal128Element) current).getValue();
        }
        throw new IllegalStateException("Current value not decimal32");
    }

    @Override
    public UUID getUUID() {
        if (current instanceof UUIDElement) {
            return ((UUIDElement) current).getValue();
        }
        throw new IllegalStateException("Current value not uuid");
    }

    @Override
    public Binary getBinary() {
        if (current instanceof BinaryElement) {
            return ((BinaryElement) current).getValue();
        }
        throw new IllegalStateException("Current value not binary");
    }

    @Override
    public String getString() {
        if (current instanceof StringElement) {
            return ((StringElement) current).getValue();
        }
        throw new IllegalStateException("Current value not string");
    }

    @Override
    public Symbol getSymbol() {
        if (current instanceof SymbolElement) {
            return ((SymbolElement) current).getValue();
        }
        throw new IllegalStateException("Current value not symbol");
    }

    @Override
    public Object getObject() {
        return current == null ? null : current.getValue();
    }

    @Override
    public Map<Object, Object> getJavaMap() {
        if (current instanceof MapElement) {
            return ((MapElement) current).getValue();
        }
        throw new IllegalStateException("Current value not map");
    }

    @Override
    public List<Object> getJavaList() {
        if (current instanceof ListElement) {
            return ((ListElement) current).getValue();
        }
        throw new IllegalStateException("Current value not list");
    }

    @Override
    public Object[] getJavaArray() {
        if (current instanceof ArrayElement) {
            return ((ArrayElement) current).getValue();
        }
        throw new IllegalStateException("Current value not array");
    }

    @Override
    public DescribedType getDescribedType() {
        if (current instanceof DescribedTypeElement) {
            return ((DescribedTypeElement) current).getValue();
        }
        throw new IllegalStateException("Current value not described type");
    }

    @Override
    public String format() {
        StringBuilder sb = new StringBuilder();
        Element<?> el = first;
        boolean first = true;
        while (el != null) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            el.render(sb);
            el = el.next();
        }

        return sb.toString();
    }

    private void render(StringBuilder sb, Element<?> el) {
        if (el == null) {
            return;
        }

        sb.append("    ").append(el).append("\n");
        if (el.canEnter()) {
            render(sb, el.child());
        }
        render(sb, el.next());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        render(sb, first);
        return String.format("Data[current=%h, parent=%h]{%n%s}", System.identityHashCode(current), System.identityHashCode(parent), sb);
    }
}
