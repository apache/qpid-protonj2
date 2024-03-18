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
package org.apache.qpid.protonj2.test.driver.matchers.types;

import java.nio.ByteBuffer;

import org.apache.qpid.protonj2.test.driver.codec.messaging.AmqpValue;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;
import org.hamcrest.Description;

public class EncodedDataMatcher extends EncodedAmqpTypeMatcher {

    private static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:data:binary");
    private static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000075L);

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     */
    public EncodedDataMatcher(byte[] expectedValue) {
        this(expectedValue != null ? new Binary(expectedValue) : null, false);
    }

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     */
    public EncodedDataMatcher(Binary expectedValue) {
        this(expectedValue, false);
    }

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     * @param permitTrailingBytes
     *        if it is permitted for bytes to be left in the Binary after
     *        consuming the {@link AmqpValue}
     */
    public EncodedDataMatcher(byte[] expectedValue, boolean permitTrailingBytes) {
        this(expectedValue != null ? new Binary(expectedValue) : null, permitTrailingBytes);
    }

    /**
     * @param expectedValue
     *        the value that is expected to be IN the received {@link Data}
     * @param permitTrailingBytes
     *        if it is permitted for bytes to be left in the Binary after
     *        consuming the {@link AmqpValue}
     */
    public EncodedDataMatcher(Binary expectedValue, boolean permitTrailingBytes) {
        super(DESCRIPTOR_SYMBOL, DESCRIPTOR_CODE, expectedValue, permitTrailingBytes);
    }

    @Override
    protected boolean matchesSafely(ByteBuffer receivedBinary) {
        return super.matchesSafely(receivedBinary);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("a Binary encoding of a Data that wraps a Binary containing: ").appendValue(getExpectedValue());
    }
}