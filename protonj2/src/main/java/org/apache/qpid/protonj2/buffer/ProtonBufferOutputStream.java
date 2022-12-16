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
package org.apache.qpid.protonj2.buffer;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * {@link ProtonBuffer} specialized {@link OutputStream} implementation which can be used to adapt
 * the proton buffer types into code that uses the streams API. This stream wrapper does not take
 * ownership of the provided buffer and will not close it upon a call to {@link #close()}
 * <p>
 * This stream is not thread-safe and should not be used by more than one thread at the same time.
 */
public class ProtonBufferOutputStream extends OutputStream implements DataOutput {

    private final ProtonBuffer buffer;
    private final int startWriteIndex;

    private DataOutputStream cachedDataOut;
    private boolean closed;

    /**
     * Create a new {@link OutputStream} which wraps the given buffer.
     *
     * @param buffer
     *      The buffer that this stream will write to.
     */
    public ProtonBufferOutputStream(ProtonBuffer buffer) {
        this.buffer = buffer;
        this.startWriteIndex = buffer.getWriteOffset();
    }

    /**
     * @return a running total of the number of bytes that has been written to this {@link OutputStream}
     */
    public int getBytesWritten() {
        return buffer.getWriteOffset() - startWriteIndex;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                super.close();
            } finally {
                this.closed = true;
                if (cachedDataOut != null) {
                    cachedDataOut.close();
                }
            }
        }
    }

    @Override
    public void writeBoolean(boolean value) throws IOException {
        prepareWrite(Byte.BYTES);
        buffer.writeBoolean(value);
    }

    @Override
    public void write(int value) throws IOException {
        prepareWrite(Byte.BYTES);
        buffer.writeByte((byte) (value & 0xFF));
    }

    @Override
    public void write(byte[] array, int offset, int length) throws IOException {
        prepareWrite(length);
        if (length != 0) {
            buffer.writeBytes(array, offset, length);
        }
    }

    @Override
    public void write(byte[] array) throws IOException {
        prepareWrite(array.length);
        buffer.writeBytes(array);
    }

    @Override
    public void writeByte(int value) throws IOException {
        checkClosed();
        prepareWrite(Byte.BYTES);
        buffer.writeByte((byte) (value & 0xFF));
    }

    @Override
    public void writeShort(int value) throws IOException {
        prepareWrite(Short.BYTES);
        buffer.writeShort((short) value);
    }

    @Override
    public void writeChar(int value) throws IOException {
        prepareWrite(Character.BYTES);
        buffer.writeShort((short) value);
    }

    @Override
    public void writeInt(int value) throws IOException {
        prepareWrite(Integer.BYTES);
        buffer.writeInt(value);
    }

    @Override
    public void writeLong(long value) throws IOException {
        prepareWrite(Long.BYTES);
        buffer.writeLong(value);
    }

    @Override
    public void writeFloat(float value) throws IOException {
        prepareWrite(Float.BYTES);
        buffer.writeFloat(value);
    }

    @Override
    public void writeDouble(double value) throws IOException {
        prepareWrite(Double.BYTES);
        buffer.writeDouble(value);
    }

    @Override
    public void writeBytes(String value) throws IOException {
        prepareWrite(value.length());
        buffer.writeBytes(value.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public void writeChars(String value) throws IOException {
        checkClosed();
        prepareWrite(value.length() * Character.BYTES);
        for (int i = 0; i < value.length(); ++i) {
            buffer.writeChar(value.charAt(i));
        }
    }

    @Override
    public void writeUTF(String value) throws IOException {
        checkClosed();
        if (cachedDataOut == null) {
            cachedDataOut = new DataOutputStream(this);
        }

        cachedDataOut.writeUTF(value);
    }

    private void prepareWrite(int size) throws IOException {
        checkClosed();
        buffer.ensureWritable(size, buffer.capacity() >> 2, true);
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("The ProtonBuffer OutputStream has been closed");
        }
    }
}
