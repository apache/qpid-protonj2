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

import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.protonj2.test.driver.codec.messaging.AmqpValue;
import org.apache.qpid.protonj2.test.driver.codec.messaging.Data;
import org.apache.qpid.protonj2.test.driver.matchers.types.EncodedPartialDataSectionMatcher;
import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class EncodedPartialDataSectionMatcherTest {

    @Test
    public void testValidatePartiallyTransmittedDataSection() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final int EXPECTED_SIZE = 256;

        ByteBuf body = Unpooled.buffer();

        body.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.writeByte(EncodingCodes.SMALLULONG);
        body.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        body.writeByte(EncodingCodes.VBIN32);
        body.writeInt(EXPECTED_SIZE);
        body.writeBytes(PAYLOAD);

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertThat(body, matcher);
    }

    @Test
    public void testValidateImcorrectPartiallyTransmittedDataSectionInvalidSize() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final int EXPECTED_SIZE = 256;

        ByteBuf body = Unpooled.buffer();

        body.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.writeByte(EncodingCodes.SMALLULONG);
        body.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        body.writeByte(EncodingCodes.VBIN32);
        body.writeInt(EXPECTED_SIZE + 1);
        body.writeBytes(PAYLOAD);

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertFalse(matcher.matches(body));
    }

    @Test
    public void testValidateImcorrectPartiallyTransmittedDataSectionInvalidDescribedType() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final int EXPECTED_SIZE = 256;

        ByteBuf body = Unpooled.buffer();

        body.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.writeByte(EncodingCodes.SMALLULONG);
        body.writeByte(AmqpValue.DESCRIPTOR_CODE.byteValue());
        body.writeByte(EncodingCodes.VBIN32);
        body.writeInt(EXPECTED_SIZE + 1);
        body.writeBytes(PAYLOAD);

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertFalse(matcher.matches(body));
    }

    @Test
    public void testValidateImcorrectPartiallyTransmittedDataSectionDescribedIsNotBinary() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final int EXPECTED_SIZE = 256;

        ByteBuf body = Unpooled.buffer();

        body.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.writeByte(EncodingCodes.SMALLULONG);
        body.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        body.writeByte(EncodingCodes.SYM8);
        body.writeInt(EXPECTED_SIZE + 1);
        body.writeBytes(PAYLOAD);

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertFalse(matcher.matches(body));
    }

    @Test
    public void testValidateImcorrectPartiallyTransmittedDataSectionInvalidBody() {
        final byte[] PAYLOAD = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        final byte[] ACTUAL_PAYLOAD = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final int EXPECTED_SIZE = 256;

        ByteBuf body = Unpooled.buffer();

        body.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
        body.writeByte(EncodingCodes.SMALLULONG);
        body.writeByte(Data.DESCRIPTOR_CODE.byteValue());
        body.writeByte(EncodingCodes.VBIN32);
        body.writeInt(EXPECTED_SIZE + 1);
        body.writeBytes(ACTUAL_PAYLOAD);

        EncodedPartialDataSectionMatcher matcher =
            new EncodedPartialDataSectionMatcher(EXPECTED_SIZE, PAYLOAD);

        assertFalse(matcher.matches(body));
    }
}
