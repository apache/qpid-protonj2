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
package org.apache.qpid.protonj2.test.driver.codec;

import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;

import io.netty.buffer.ByteBuf;

class ArrayElement extends AbstractElement<Object[]> {

    private final boolean described;
    private final Codec.DataType arrayType;
    private ConstructorType constructorType;
    private Element<?> first;

    enum ConstructorType {
        TINY, SMALL, LARGE
    }

    static ConstructorType TINY = ConstructorType.TINY;
    static ConstructorType SMALL = ConstructorType.SMALL;
    static ConstructorType LARGE = ConstructorType.LARGE;

    ArrayElement(Element<?> parent, Element<?> prev, boolean described, Codec.DataType type) {
        super(parent, prev);
        this.described = described;
        this.arrayType = type;
        if (arrayType == null) {
            throw new NullPointerException("Array type cannot be null");
        } else if (arrayType == Codec.DataType.DESCRIBED) {
            throw new IllegalArgumentException("Array type cannot be DESCRIBED");
        }
        switch (arrayType) {
            case UINT:
            case ULONG:
            case LIST:
                setConstructorType(TINY);
                break;
            default:
                setConstructorType(SMALL);
        }
    }

    ConstructorType constructorType() {
        return constructorType;
    }

    void setConstructorType(ConstructorType type) {
        constructorType = type;
    }

    @Override
    public int size() {
        ConstructorType oldConstructorType;
        int bodySize;
        int count = 0;
        do {
            bodySize = 1; // data type constructor
            oldConstructorType = constructorType;
            Element<?> element = first;
            while (element != null) {
                count++;
                bodySize += element.size();
                element = element.next();
            }
        } while (oldConstructorType != constructorType());

        if (isDescribed()) {
            bodySize++; // 00 instruction
            if (count != 0) {
                count--;
            }
        }

        if (isElementOfArray()) {
            ArrayElement parent = (ArrayElement) parent();
            if (parent.constructorType() == SMALL) {
                if (count <= 255 && bodySize <= 254) {
                    bodySize += 2;
                } else {
                    parent.setConstructorType(LARGE);
                    bodySize += 8;
                }
            } else {
                bodySize += 8;
            }
        } else {

            if (count <= 255 && bodySize <= 254) {
                bodySize += 3;
            } else {
                bodySize += 9;
            }

        }

        return bodySize;
    }

    @Override
    public Object[] getValue() {
        if (isDescribed()) {
            DescribedType[] rVal = new DescribedType[(int) count()];
            Object descriptor = first == null ? null : first.getValue();
            Element<?> element = first == null ? null : first.next();
            int i = 0;
            while (element != null) {
                rVal[i++] = new DescribedTypeImpl(descriptor, element.getValue());
                element = element.next();
            }
            return rVal;
        } else if (arrayType == Codec.DataType.SYMBOL) {
            Symbol[] rVal = new Symbol[(int) count()];
            SymbolElement element = (SymbolElement) first;
            int i = 0;
            while (element != null) {
                rVal[i++] = element.getValue();
                element = (SymbolElement) element.next();
            }
            return rVal;
        } else {
            Object[] rVal = new Object[(int) count()];
            Element<?> element = first;
            int i = 0;
            while (element != null) {
                rVal[i++] = element.getValue();
                element = element.next();
            }
            return rVal;
        }
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.ARRAY;
    }

    @Override
    public int encode(ByteBuf buffer) {
        int size = size();

        final int count = (int) count();

        if (buffer.maxWritableBytes() >= size) {
            if (!isElementOfArray()) {
                if (size > 257 || count > 255) {
                    buffer.writeByte((byte) 0xf0);
                    buffer.writeInt(size - 5);
                    buffer.writeInt(count);
                } else {
                    buffer.writeByte((byte) 0xe0);
                    buffer.writeByte((byte) (size - 2));
                    buffer.writeByte((byte) count);
                }
            } else {
                ArrayElement parent = (ArrayElement) parent();
                if (parent.constructorType() == SMALL) {
                    buffer.writeByte((byte) (size - 1));
                    buffer.writeByte((byte) count);
                } else {
                    buffer.writeInt(size - 4);
                    buffer.writeInt(count);
                }
            }
            Element<?> element = first;
            if (isDescribed()) {
                buffer.writeByte((byte) 0);
                if (element == null) {
                    buffer.writeByte((byte) 0x40);
                } else {
                    element.encode(buffer);
                    element = element.next();
                }
            }
            switch (arrayType) {
                case NULL:
                    buffer.writeByte((byte) 0x40);
                    break;
                case BOOL:
                    buffer.writeByte((byte) 0x56);
                    break;
                case UBYTE:
                    buffer.writeByte((byte) 0x50);
                    break;
                case BYTE:
                    buffer.writeByte((byte) 0x51);
                    break;
                case USHORT:
                    buffer.writeByte((byte) 0x60);
                    break;
                case SHORT:
                    buffer.writeByte((byte) 0x61);
                    break;
                case UINT:
                    switch (constructorType()) {
                        case TINY:
                            buffer.writeByte((byte) 0x43);
                            break;
                        case SMALL:
                            buffer.writeByte((byte) 0x52);
                            break;
                        case LARGE:
                            buffer.writeByte((byte) 0x70);
                            break;
                    }
                    break;
                case INT:
                    buffer.writeByte(constructorType == SMALL ? (byte) 0x54 : (byte) 0x71);
                    break;
                case CHAR:
                    buffer.writeByte((byte) 0x73);
                    break;
                case ULONG:
                    switch (constructorType()) {
                        case TINY:
                            buffer.writeByte((byte) 0x44);
                            break;
                        case SMALL:
                            buffer.writeByte((byte) 0x53);
                            break;
                        case LARGE:
                            buffer.writeByte((byte) 0x80);
                            break;
                    }
                    break;
                case LONG:
                    buffer.writeByte(constructorType == SMALL ? (byte) 0x55 : (byte) 0x81);
                    break;
                case TIMESTAMP:
                    buffer.writeByte((byte) 0x83);
                    break;
                case FLOAT:
                    buffer.writeByte((byte) 0x72);
                    break;
                case DOUBLE:
                    buffer.writeByte((byte) 0x82);
                    break;
                case DECIMAL32:
                    buffer.writeByte((byte) 0x74);
                    break;
                case DECIMAL64:
                    buffer.writeByte((byte) 0x84);
                    break;
                case DECIMAL128:
                    buffer.writeByte((byte) 0x94);
                    break;
                case UUID:
                    buffer.writeByte((byte) 0x98);
                    break;
                case BINARY:
                    buffer.writeByte(constructorType == SMALL ? (byte) 0xa0 : (byte) 0xb0);
                    break;
                case STRING:
                    buffer.writeByte(constructorType == SMALL ? (byte) 0xa1 : (byte) 0xb1);
                    break;
                case SYMBOL:
                    buffer.writeByte(constructorType == SMALL ? (byte) 0xa3 : (byte) 0xb3);
                    break;
                case ARRAY:
                    buffer.writeByte(constructorType == SMALL ? (byte) 0xe0 : (byte) 0xf0);
                    break;
                case LIST:
                    buffer.writeByte(constructorType == TINY ? (byte) 0x45 : constructorType == SMALL ? (byte) 0xc0 : (byte) 0xd0);
                    break;
                case MAP:
                    buffer.writeByte(constructorType == SMALL ? (byte) 0xc1 : (byte) 0xd1);
                    break;
                case DESCRIBED:
                    break;
                default:
                    break;
            }
            while (element != null) {
                element.encode(buffer);
                element = element.next();
            }
            return size;
        } else {
            return 0;
        }
    }

    @Override
    public boolean canEnter() {
        return true;
    }

    @Override
    public Element<?> child() {
        return first;
    }

    @Override
    public void setChild(Element<?> elt) {
        first = elt;
    }

    @Override
    public Element<?> addChild(Element<?> element) {
        if (isDescribed() || element.getDataType() == arrayType) {
            first = element;
            return element;
        } else {
            Element<?> replacement = coerce(element);
            if (replacement != null) {
                first = replacement;
                return replacement;
            }
            throw new IllegalArgumentException("Attempting to add instance of " + element.getDataType() + " to array of " + arrayType);
        }
    }

    private Element<?> coerce(Element<?> element) {
        switch (arrayType) {
            case INT:
                int i;
                switch (element.getDataType()) {
                    case BYTE:
                        i = ((ByteElement) element).getValue().intValue();
                        break;
                    case SHORT:
                        i = ((ShortElement) element).getValue().intValue();
                        break;
                    case LONG:
                        i = ((LongElement) element).getValue().intValue();
                        break;
                    default:
                        return null;
                }
                return new IntegerElement(element.parent(), element.prev(), i);
            case LONG:
                long l;
                switch (element.getDataType()) {
                    case BYTE:
                        l = ((ByteElement) element).getValue().longValue();
                        break;
                    case SHORT:
                        l = ((ShortElement) element).getValue().longValue();
                        break;
                    case INT:
                        l = ((IntegerElement) element).getValue().longValue();
                        break;
                    default:
                        return null;
                }
                return new LongElement(element.parent(), element.prev(), l);
            case ARRAY:
                break;
            case BINARY:
                break;
            case BOOL:
                break;
            case BYTE:
                break;
            case CHAR:
                break;
            case DECIMAL128:
                break;
            case DECIMAL32:
                break;
            case DECIMAL64:
                break;
            case DESCRIBED:
                break;
            case DOUBLE:
                break;
            case FLOAT:
                break;
            case LIST:
                break;
            case MAP:
                break;
            case NULL:
                break;
            case SHORT:
                break;
            case STRING:
                break;
            case SYMBOL:
                break;
            case TIMESTAMP:
                break;
            case UBYTE:
                break;
            case UINT:
                break;
            case ULONG:
                break;
            case USHORT:
                break;
            case UUID:
                break;
            default:
                break;
        }
        return null;
    }

    @Override
    public Element<?> checkChild(Element<?> element) {
        if (element.getDataType() != arrayType) {
            Element<?> replacement = coerce(element);
            if (replacement != null) {
                return replacement;
            }
            throw new IllegalArgumentException("Attempting to add instance of " + element.getDataType() + " to array of " + arrayType);
        }
        return element;
    }

    public long count() {
        int count = 0;
        Element<?> elt = first;
        while (elt != null) {
            count++;
            elt = elt.next();
        }
        if (isDescribed() && count != 0) {
            count--;
        }
        return count;
    }

    public boolean isDescribed() {
        return described;
    }

    public Codec.DataType getArrayDataType() {
        return arrayType;
    }

    @Override
    String startSymbol() {
        return String.format("%s%s[", isDescribed() ? "D" : "", getArrayDataType());
    }

    @Override
    String stopSymbol() {
        return "]";
    }
}
