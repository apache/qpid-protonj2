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

    public static final int PROTOCOL_ID_INDEX = 4;
    public static final int MAJOR_VERSION_INDEX = 5;
    public static final int MINOR_VERSION_INDEX = 6;
    public static final int REVISION_INDEX = 7;

    public static final byte AMQP_PROTOCOL_ID = 0;
    public static final byte SASL_PROTOCOL_ID = 3;

    public static final int HEADER_SIZE_BYTES = 8;

    private static final AMQPHeader AMQP_HEADER =
        new AMQPHeader(new byte[] { 'A', 'M', 'Q', 'P', 0, 1, 0, 0 });

    private static final AMQPHeader SASL_HEADER =
        new AMQPHeader(new byte[] { 'A', 'M', 'Q', 'P', 3, 1, 0, 0 });

    private ProtonBuffer buffer;

    public AMQPHeader() {
        this(AMQP_HEADER.buffer);
    }

    public AMQPHeader(byte[] headerBytes) {
        setBuffer(new ProtonByteBuffer(headerBytes), true);
    }

    public AMQPHeader(ProtonBuffer buffer) {
        setBuffer(new ProtonByteBuffer(HEADER_SIZE_BYTES).writeBytes(buffer), true);
    }

    public AMQPHeader(ProtonBuffer buffer, boolean validate) {
        setBuffer(new ProtonByteBuffer(HEADER_SIZE_BYTES).writeBytes(buffer), validate);
    }

    public static AMQPHeader getAMQPHeader() {
        return AMQP_HEADER;
    }

    public static AMQPHeader getSASLHeader() {
        return SASL_HEADER;
    }

    public int getProtocolId() {
        return buffer.getByte(PROTOCOL_ID_INDEX) & 0xFF;
    }

    public int getMajor() {
        return buffer.getByte(MAJOR_VERSION_INDEX) & 0xFF;
    }

    public int getMinor() {
        return buffer.getByte(MINOR_VERSION_INDEX) & 0xFF;
    }

    public int getRevision() {
        return buffer.getByte(REVISION_INDEX) & 0xFF;
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

    public boolean isSaslHeader() {
        return getProtocolId() == SASL_PROTOCOL_ID;
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
        if (validate) {
            if (value.getReadableBytes() != 8 || !startsWith(value, PREFIX)) {
                throw new IllegalArgumentException("Not an AMQP header buffer");
            }

            byte current = value.getByte(PROTOCOL_ID_INDEX);
            if (current != AMQP_PROTOCOL_ID && current != SASL_PROTOCOL_ID) {
                throw new IllegalArgumentException(String.format(
                    "Invalid protocol Id specified %d : expected one of %d or %d",
                    current, AMQP_PROTOCOL_ID, SASL_PROTOCOL_ID));
            }

            current = value.getByte(MAJOR_VERSION_INDEX);
            if (current != 1) {
                throw new IllegalArgumentException(String.format(
                    "Invalid Major version specified %d : expected %d", current, 1));
            }

            current = value.getByte(MINOR_VERSION_INDEX);
            if (current != 0) {
                throw new IllegalArgumentException(String.format(
                    "Invalid Minor version specified %d : expected %d", current, 0));
            }

            current = value.getByte(REVISION_INDEX);
            if (current != 0) {
                throw new IllegalArgumentException(String.format(
                    "Invalid revision specified %d : expected %d", current, 0));
            }
        }

        buffer = value;
    }

    /**
     * Provide this AMQP Header with a handler that will process the given AMQP header
     * depending on the protocol type the correct handler method is invoked.
     *
     * @param handler
     *      The {@link HeaderHandler} instance to use to process the header.
     * @param context
     *      A context object to pass along with the header.
     */
    public <E> void invoke(HeaderHandler<E> handler, E context) {
        if (isSaslHeader()) {
            handler.handleSASLHeader(this, context);
        } else {
            handler.handleAMQPHeader(this, context);
        }
    }

    public interface HeaderHandler<E> {

        default void handleAMQPHeader(AMQPHeader header, E context) {}

        default void handleSASLHeader(AMQPHeader header, E context) {}

    }
}
