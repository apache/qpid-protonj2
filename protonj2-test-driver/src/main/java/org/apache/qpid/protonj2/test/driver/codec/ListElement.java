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

import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class ListElement extends AbstractElement<List<Object>> {

    private Element<?> first;

    ListElement(Element<?> parent, Element<?> prev) {
        super(parent, prev);
    }

    public int count() {
        int count = 0;
        Element<?> elt = first;
        while (elt != null) {
            count++;
            elt = elt.next();
        }
        return count;
    }

    @Override
    public int size() {
        int count = 0;
        int size = 0;
        Element<?> elt = first;
        while (elt != null) {
            count++;
            size += elt.size();
            elt = elt.next();
        }
        if (isElementOfArray()) {
            ArrayElement parent = (ArrayElement) parent();
            if (parent.constructorType() == ArrayElement.TINY) {
                if (count != 0) {
                    parent.setConstructorType(ArrayElement.ConstructorType.SMALL);
                    size += 2;
                }
            } else if (parent.constructorType() == ArrayElement.SMALL) {
                if (count > 255 || size > 254) {
                    parent.setConstructorType(ArrayElement.ConstructorType.LARGE);
                    size += 8;
                } else {
                    size += 2;
                }
            } else {
                size += 8;
            }

        } else {
            if (count == 0) {
                size = 1;
            } else if (count <= 255 && size <= 254) {
                size += 3;
            } else {
                size += 9;
            }
        }

        return size;
    }

    @Override
    public List<Object> getValue() {
        List<Object> list = new ArrayList<>();
        Element<?> elt = first;
        while (elt != null) {
            list.add(elt.getValue());
            elt = elt.next();
        }

        return Collections.unmodifiableList(list);
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.LIST;
    }

    @Override
    public int encode(DataOutput output) {
        try {
            int encodedSize = size();

            int count = 0;
            int size = 0;
            Element<?> elt = first;
            while (elt != null) {
                count++;
                size += elt.size();
                elt = elt.next();
            }

            if (isElementOfArray()) {
                switch (((ArrayElement) parent()).constructorType()) {
                    case TINY:
                        break;
                    case SMALL:
                        output.writeByte((byte) (size + 1));
                        output.writeByte((byte) count);
                        break;
                    case LARGE:
                        output.writeInt((size + 4));
                        output.writeInt(count);
                }
            } else {
                if (count == 0) {
                    output.writeByte((byte) 0x45);
                } else if (size <= 254 && count <= 255) {
                    output.writeByte((byte) 0xc0);
                    output.writeByte((byte) (size + 1));
                    output.writeByte((byte) count);
                } else {
                    output.writeByte((byte) 0xd0);
                    output.writeInt((size + 4));
                    output.writeInt(count);
                }
            }

            elt = first;
            while (elt != null) {
                elt.encode(output);
                elt = elt.next();
            }

            return encodedSize;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
    public Element<?> checkChild(Element<?> element) {
        return element;
    }

    @Override
    public Element<?> addChild(Element<?> element) {
        first = element;
        return element;
    }

    @Override
    String startSymbol() {
        return "[";
    }

    @Override
    String stopSymbol() {
        return "]";
    }
}
