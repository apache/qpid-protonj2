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
package org.apache.qpid.protonj2.test.driver.legacy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.transport.Transfer;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.message.Message;

/**
 * Generates the test data used to create a tests for codec that read
 * Frames encoded using the proton-j framework.
 */
public class LegacyCodecTransferFramesTestDataGenerator {

    public static void main(String[] args) {
        // 1: Transfer frame for complete delivery
        Transfer completedTransfer = new Transfer();
        completedTransfer.setAborted(false);
        completedTransfer.setMore(false);
        completedTransfer.setDeliveryId(UnsignedInteger.valueOf(1));
        completedTransfer.setHandle(UnsignedInteger.valueOf(2));
        completedTransfer.setDeliveryTag(new Binary(new byte[] { 0, 1 }));
        completedTransfer.setMessageFormat(null);
        completedTransfer.setSettled(true);

        String emptyOpenFrameString = LegacyFrameDataGenerator.generateUnitTestVariable("completedTransfer", completedTransfer, encodeMessage());
        System.out.println(emptyOpenFrameString);
    }

    private static ReadableBuffer encodeMessage() {
        final WritableBuffer.ByteBufferWrapper buffer = WritableBuffer.ByteBufferWrapper.allocate(1024);
        final byte[] body = new byte[100];
        Arrays.fill(body, (byte) 'A');

        Message message = Message.Factory.create();

        ApplicationProperties properties = new ApplicationProperties(new HashMap<>());
        properties.getValue().put("timestamp", "123456789");

        message.setAddress("test");
        message.setApplicationProperties(properties);
        message.setBody(new Data(new Binary(body)));
        message.setMessageId(UUID.randomUUID());

        message.encode(buffer);

        return buffer.toReadableBuffer();
    }
}
