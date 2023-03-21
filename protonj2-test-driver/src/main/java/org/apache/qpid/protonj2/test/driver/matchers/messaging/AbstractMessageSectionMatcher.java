/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.qpid.protonj2.test.driver.matchers.messaging;

import static org.hamcrest.MatcherAssert.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.qpid.protonj2.test.driver.codec.Codec;
import org.apache.qpid.protonj2.test.driver.codec.primitives.DescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.hamcrest.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMessageSectionMatcher {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final UnsignedLong numericDescriptor;
    private final Symbol symbolicDescriptor;

    private final Map<Object, Matcher<?>> fieldMatchers;
    private Map<Object, Object> receivedFields;

    private final boolean expectTrailingBytes;

    protected AbstractMessageSectionMatcher(UnsignedLong numericDescriptor, Symbol symbolicDescriptor, Map<Object, Matcher<?>> fieldMatchers, boolean expectTrailingBytes) {
        this.numericDescriptor = numericDescriptor;
        this.symbolicDescriptor = symbolicDescriptor;
        this.fieldMatchers = fieldMatchers;
        this.expectTrailingBytes = expectTrailingBytes;
    }

    protected Map<Object, Matcher<?>> getMatchers() {
        return fieldMatchers;
    }

    protected Map<Object, Object> getReceivedFields() {
        return receivedFields;
    }

    /**
     * @param receivedBinary
     *        The received Binary value that should be validated.
     *
     * @return the number of bytes consumed from the provided Binary
     *
     * @throws RuntimeException
     *         if the provided Binary does not match expectation in some way
     */
    public int verify(ByteBuffer receivedBinary) throws RuntimeException {
        final int length = receivedBinary.remaining();
        final Codec data = Codec.Factory.create();
        final long decoded = data.decode(receivedBinary);

        if (decoded > Integer.MAX_VALUE) {
            throw new IllegalStateException("Decoded more bytes than Binary supports holding");
        }

        if (decoded < length && !expectTrailingBytes) {
            throw new IllegalArgumentException(
                "Expected to consume all bytes, but trailing bytes remain: Got " + length + ", consumed " + decoded);
        }

        DescribedType decodedDescribedType = data.getDescribedType();
        verifyReceivedDescribedType(decodedDescribedType);

        // Need to cast to int, but verified earlier that it is <
        // Integer.MAX_VALUE
        return (int) decoded;
    }

    private void verifyReceivedDescribedType(DescribedType decodedDescribedType) {
        Object descriptor = decodedDescribedType.getDescriptor();
        if (!(symbolicDescriptor.equals(descriptor) || numericDescriptor.equals(descriptor))) {
            throw new IllegalArgumentException(
                "Unexpected section type descriptor. Expected " + symbolicDescriptor +
                " or " + numericDescriptor + ", but got: " + descriptor);
        }

        verifyReceivedDescribedObject(decodedDescribedType.getDescribed());
    }

    /**
     * sub-classes should implement depending on the expected content of the
     * particular section type.
     *
     * @param describedObject
     * 		the described type instance to validate.
     */
    protected abstract void verifyReceivedDescribedObject(Object describedObject);

    /**
     * Utility method for use by sub-classes that expect field-based sections,
     * i.e lists or maps.
     *
     * @param valueMap
     * 		the key / value map of fields that comprise the received section.
     */
    protected void verifyReceivedFields(Map<Object, Object> valueMap) {
        receivedFields = valueMap;

        LOG.debug("About to check the fields of the section." + "\n  Received:" + valueMap + "\n  Expectations: " + fieldMatchers);
        for (Map.Entry<Object, Matcher<?>> entry : fieldMatchers.entrySet()) {
            @SuppressWarnings("unchecked")
            Matcher<Object> matcher = (Matcher<Object>) entry.getValue();
            Object field = entry.getKey();
            assertThat("Field " + field + " value should match", valueMap.get(field), matcher);
        }
    }

    /**
     * Intended to be overridden in most cases that use the above method (but
     * not necessarily all - hence not marked as abstract)
     *
     * @param fieldIndex
     *     the field index in the list of section fields whose {@link Enum} constant is being looked up.
     *
     * @return the field enumeration value mapped to the provided index.
     */
    protected Enum<?> getField(int fieldIndex) {
        throw new UnsupportedOperationException("getFieldName is expected to be overridden by subclass if it is required");
    }
}