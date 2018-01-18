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
package org.apache.qpid.proton4j.amqp.transport;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBuffer;

/**
 * Represents the AMQP protocol handshake packet that is sent during the
 * initial exchange with a remote peer.
 */
public class AMQPHeader {

    static final byte[] PREFIX = new byte[] { 'A', 'M', 'Q', 'P' };

    private static final AMQPHeader AMQP_HEADER =
        new AMQPHeader(new byte[] { 'A', 'M', 'Q', 'P', 0, 1, 0, 0 });

    private static final AMQPHeader SASL_HEADER =
        new AMQPHeader(new byte[] { 'A', 'M', 'Q', 'P', 3, 1, 0, 0 });

    private ProtonBuffer buffer;

    public AMQPHeader() {
        this(new ProtonByteBuffer(new byte[] { 'A', 'M', 'Q', 'P', 0, 1, 0, 0 }));
    }

    public AMQPHeader(byte[] headerBytes) {
        setBuffer(new ProtonByteBuffer(headerBytes), true);
    }

    public AMQPHeader(ProtonBuffer buffer) {
        this(buffer.copy(), true);
    }

    public AMQPHeader(ProtonBuffer buffer, boolean validate) {
        setBuffer(buffer.copy(), validate);
    }

    public static AMQPHeader getRawAMQPHeader() {
        return AMQP_HEADER;
    }

    public static AMQPHeader getSASLHeader() {
        return SASL_HEADER;
    }

    public int getProtocolId() {
        return buffer.getByte(4) & 0xFF;
    }

    public int getMajor() {
        return buffer.getByte(5) & 0xFF;
    }

    public int getMinor() {
        return buffer.getByte(6) & 0xFF;
    }

    public int getRevision() {
        return buffer.getByte(7) & 0xFF;
    }

    public ProtonBuffer getBuffer() {
        return buffer.copy();
    }

    public byte getByteAt(int i) {
        return buffer.getByte(i);
    }

    public boolean hasValidPrefix() {
        return startsWith(buffer, PREFIX);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < buffer.getReadableBytes(); ++i) {
            char value = (char) buffer.getByte(i);
            if (Character.isLetter(value)) {
                builder.append(value);
            } else {
                builder.append(",");
                builder.append((int) value);
            }
        }
        return builder.toString();
    }

    private boolean startsWith(ProtonBuffer buffer, byte[] value) {
        if (buffer == null || buffer.getReadableBytes() < value.length) {
            return false;
        }

        for (int i = 0; i < value.length; ++i) {
            if (buffer.getByte(i) != value[i]) {
                return false;
            }
        }

        return true;
    }

    private void setBuffer(ProtonBuffer value, boolean validate) {
        if (validate && (value.getReadableBytes() != 8 || !startsWith(value, PREFIX))) {
            throw new IllegalArgumentException("Not an AMQP header buffer");
        }

        buffer = value.duplicate();
    }
}
