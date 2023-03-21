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
import java.nio.charset.Charset;

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
    public int encode(DataOutput output) {
        try {
            final byte[] bytes = value.getBytes(UTF_8);
            final int length = bytes.length;

            int size = size(length);

            if (isElementOfArray()) {
                final ArrayElement parent = (ArrayElement) parent();

                if (parent.constructorType() == ArrayElement.SMALL) {
                    output.writeByte((byte) length);
                } else {
                    output.writeInt(length);
                }
            } else if (length <= 255) {
                output.writeByte((byte) 0xa1);
                output.writeByte((byte) length);
            } else {
                output.writeByte((byte) 0xb1);
                output.writeInt(length);
            }
            output.write(bytes);
            return size;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
