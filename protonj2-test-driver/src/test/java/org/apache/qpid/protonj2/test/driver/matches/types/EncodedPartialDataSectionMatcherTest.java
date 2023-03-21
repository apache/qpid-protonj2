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
package org.apache.qpid.protonj2.test.driver.matches.types;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.protonj2.test.driver.codec.messaging.AmqpValue;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedPartialDataSectionMatcher;
import org.junit.jupiter.api.Test;

public class EncodedPartialDataSectionMatcherTest {

    @Test
    public void testValidatePartiallyTransmittedDataSection() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final int EXPECTED_SIZE = 256;

        final ByteBuffer body = ByteBuffer.allocate(EXPECTED_SIZE);

        body.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.put(EncodingCodes.SMALLULONG);
        body.put(Data.DESCRIPTOR_CODE.byteValue());
        body.put(EncodingCodes.VBIN32);
        body.putInt(EXPECTED_SIZE);
        body.put(PAYLOAD);
        body.flip();

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertThat(body, matcher);
    }

    @Test
    public void testValidateIncorrectPartiallyTransmittedDataSectionInvalidSize() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final int EXPECTED_SIZE = 256;

        final ByteBuffer body = ByteBuffer.allocate(EXPECTED_SIZE);

        body.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.put(EncodingCodes.SMALLULONG);
        body.put(Data.DESCRIPTOR_CODE.byteValue());
        body.put(EncodingCodes.VBIN32);
        body.putInt(EXPECTED_SIZE + 1);
        body.put(PAYLOAD);
        body.flip();

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertFalse(matcher.matches(body));
    }

    @Test
    public void testValidateIncorrectPartiallyTransmittedDataSectionInvalidDescribedType() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final int EXPECTED_SIZE = 256;

        final ByteBuffer body = ByteBuffer.allocate(EXPECTED_SIZE);

        body.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.put(EncodingCodes.SMALLULONG);
        body.put(AmqpValue.DESCRIPTOR_CODE.byteValue());
        body.put(EncodingCodes.VBIN32);
        body.putInt(EXPECTED_SIZE + 1);
        body.put(PAYLOAD);
        body.flip();

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertFalse(matcher.matches(body));
    }

    @Test
    public void testValidateIncorrectPartiallyTransmittedDataSectionDescribedIsNotBinary() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final int EXPECTED_SIZE = 256;

        final ByteBuffer body = ByteBuffer.allocate(EXPECTED_SIZE);

        body.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.put(EncodingCodes.SMALLULONG);
        body.put(Data.DESCRIPTOR_CODE.byteValue());
        body.put(EncodingCodes.SYM8);
        body.putInt(EXPECTED_SIZE + 1);
        body.put(PAYLOAD);
        body.flip();

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertFalse(matcher.matches(body));
    }

    @Test
    public void testValidateIncorrectPartiallyTransmittedDataSectionInvalidBody() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final byte[] ACTUAL_PAYLOAD = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final int EXPECTED_SIZE = 256;

        final ByteBuffer body = ByteBuffer.allocate(EXPECTED_SIZE);

        body.put(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.put(EncodingCodes.SMALLULONG);
        body.put(Data.DESCRIPTOR_CODE.byteValue());
        body.put(EncodingCodes.VBIN32);
        body.putInt(EXPECTED_SIZE + 1);
        body.put(ACTUAL_PAYLOAD);
        body.flip();

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertFalse(matcher.matches(body));
    }
}
