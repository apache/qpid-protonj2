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
package org.apache.qpid.proton4j.test.driver.legacy;

import java.nio.ByteBuffer;

import org.apache.qpid.proton.codec.AMQPDefinedTypes;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;

/**
 * Generates test data that can be used to drive comparability tests
 * between new and old codec and framing implementation.
 */
public class LegacyFrmaeDataGenerator {

    private final static DecoderImpl decoder = new DecoderImpl();
    private final static EncoderImpl encoder = new EncoderImpl(decoder);

    private static final int DEFAULT_MAX_FRAME_SIZE = 32767;

    private static final byte AMQP_FRAME = 0;

    static {
        AMQPDefinedTypes.registerAllTypes(decoder, encoder);
    }

    public static String generateUnitTestVariable(String varName, Object protonType) {
        StringBuilder builder = new StringBuilder();

        builder.append("    // ").append("Frame data for: ")
               .append(protonType.getClass().getSimpleName()).append("\n");
        builder.append("    // ").append("  ").append(protonType.toString()).append("\n");

        // Create variable for test
        builder.append("    final byte[] ")
               .append(varName)
               .append(" = new byte[] {")
               .append(generateFrameEncoding(protonType))
               .append("};");

        return builder.toString();
    }

    public static String generateFrameEncoding(Object protonType) {
        StringBuilder builder = new StringBuilder();
        generateFrameEncodingFromProtonType(protonType, builder);
        return builder.toString();
    }

    private static void generateFrameEncodingFromProtonType(Object instance, StringBuilder builder) {
        FrameWriter writer = new FrameWriter(encoder, DEFAULT_MAX_FRAME_SIZE, AMQP_FRAME);
        ByteBuffer buffer = ByteBuffer.allocate(DEFAULT_MAX_FRAME_SIZE);

        writer.writeFrame(0, instance, null, null);
        int frameSize = writer.readBytes(buffer);

        for (int i = 0; i < frameSize; i++) {
            builder.append(buffer.get(i));
            if (i < frameSize - 1) {
                builder.append(", ");
            }
        }
    }
}
