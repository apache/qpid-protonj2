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

import java.nio.ByteBuffer;

import org.apache.qpid.proton4j.amqp.DescribedType;

class DescribedTypeElement extends AbstractElement<DescribedType> {

    private Element<?> first;

    DescribedTypeElement(Element<?> parent, Element<?> prev) {
        super(parent, prev);
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
            throw new IllegalArgumentException("Cannot add described type members to an array");
        } else if (count > 2) {
            throw new IllegalArgumentException("Too many elements in described type");
        } else if (count == 0) {
            size = 3;
        } else if (count == 1) {
            size += 2;
        } else {
            size += 1;
        }

        return size;
    }

    @Override
    public DescribedType getValue() {
        final Object descriptor = first == null ? null : first.getValue();
        Element<?> second = first == null ? null : first.next();
        final Object described = second == null ? null : second.getValue();
        return new DescribedTypeImpl(descriptor, described);
    }

    @Override
    public Data.DataType getDataType() {
        return Data.DataType.DESCRIBED;
    }

    @Override
    public int encode(ByteBuffer b) {
        int encodedSize = size();

        if (encodedSize > b.remaining()) {
            return 0;
        } else {
            b.put((byte) 0);
            if (first == null) {
                b.put((byte) 0x40);
                b.put((byte) 0x40);
            } else {
                first.encode(b);
                if (first.next() == null) {
                    b.put((byte) 0x40);
                } else {
                    first.next().encode(b);
                }
            }
        }
        return encodedSize;
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
        if (element.prev() != first) {
            throw new IllegalArgumentException("Described Type may only have two elements");
        }
        return element;

    }

    @Override
    public Element<?> addChild(Element<?> element) {
        first = element;
        return element;
    }

    @Override
    String startSymbol() {
        return "(";
    }

    @Override
    String stopSymbol() {
        return ")";
    }
}
