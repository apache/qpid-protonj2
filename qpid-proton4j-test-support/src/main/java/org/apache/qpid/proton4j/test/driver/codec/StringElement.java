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
package org.apache.qpid.proton4j.test.driver.codec;

import java.nio.charset.Charset;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

class StringElement extends AtomicElement<String> {

    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private final String value;

    StringElement(Element<?> parent, Element<?> prev, String s) {
        super(parent, prev);
        value = s;
    }

    @Override
    public int size() {
        final int length = value.getBytes(UTF_8).length;

        return size(length);
    }

    private int size(int length) {
        if (isElementOfArray()) {
            final ArrayElement parent = (ArrayElement) parent();

            if (parent.constructorType() == ArrayElement.SMALL) {
                if (length > 255) {
                    parent.setConstructorType(ArrayElement.LARGE);
                    return 4 + length;
                } else {
                    return 1 + length;
                }
            } else {
                return 4 + length;
            }
        } else {
            if (length > 255) {
                return 5 + length;
            } else {
                return 2 + length;
            }
        }
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Codec.DataType getDataType() {
        return Codec.DataType.STRING;
    }

    @Override
    public int encode(ProtonBuffer buffer) {
        final byte[] bytes = value.getBytes(UTF_8);
        final int length = bytes.length;

        int size = size(length);
        if (buffer.getMaxWritableBytes() < size) {
            return 0;
        }
        if (isElementOfArray()) {
            final ArrayElement parent = (ArrayElement) parent();

            if (parent.constructorType() == ArrayElement.SMALL) {
                buffer.writeByte((byte) length);
            } else {
                buffer.writeInt(length);
            }
        } else if (length <= 255) {
            buffer.writeByte((byte) 0xa1);
            buffer.writeByte((byte) length);
        } else {
            buffer.writeByte((byte) 0xb1);
            buffer.writeInt(length);
        }
        buffer.writeBytes(bytes);
        return size;
    }
}
