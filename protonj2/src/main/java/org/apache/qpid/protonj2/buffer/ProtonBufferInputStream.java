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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * An InputStream that can be used to adapt a {@link ProtonBuffer} for use in the
 * standard streams API. This stream wrapper assumes ownership of the provided stream
 * and will close it upon a call to the {@link #close()} method.
 */
public class ProtonBufferInputStream extends InputStream implements DataInput {

    private static final int NOT_SET = -1;

    private final ProtonBuffer buffer;
    private final int initialReadIndex;
    private int readOffsetMark = NOT_SET;

    private boolean closed;

    /**
     * Creates a new {@link InputStream} instance that wraps the given {@link ProtonBuffer}
     * and assumes ownership.
     *
     * @param buffer
     * 		The {@link ProtonBuffer} that this {@link InputStream} will read from.
     */
    public ProtonBufferInputStream(ProtonBuffer buffer) {
        Objects.requireNonNull(buffer, "The given ProtonBuffer to wrap cannot be null");
        this.initialReadIndex = buffer.getReadOffset();
        this.buffer = buffer.transfer();
    }

    /**
     * @return a running total of the number of bytes that has been read from this {@link InputStream}.
     */
    public int getBytesRead() {
        return buffer.getReadOffset() - initialReadIndex;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
            this.buffer.close();
        } finally {
            this.closed = true;
        }
    }

    @Override
    public int available() throws IOException {
        return buffer.getReadableBytes();
    }

    @Override
    public void mark(int readlimit) {
        readOffsetMark = buffer.getReadOffset();
    }

    @Override
    public void reset() throws IOException {
        if (readOffsetMark == NOT_SET) {
            throw new IOException("The stream has not been marked or the mark was invalidated");
        }
        buffer.setReadOffset(readOffsetMark);
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        if (buffer.getReadableBytes() == 0) {
            return -1;
        }

        return buffer.readByte() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkClosed();

        final int available = available();
        if (available == 0) {
            return -1;
        }

        len = Math.min(available, len);
        buffer.readBytes(b, off, len);
        return len;
    }

    @Override
    public long skip(long skipAmount) throws IOException {
        checkClosed();
        if (skipAmount > Integer.MAX_VALUE) {
            return skipBytes(Integer.MAX_VALUE);
        } else {
            return skipBytes((int) skipAmount);
        }
    }

    @Override
    public int skipBytes(int skipAmount) throws IOException {
        checkClosed();
        final int nBytes = Math.min(available(), skipAmount);
        buffer.advanceReadOffset(nBytes);
        return nBytes;
    }

    @Override
    public void readFully(byte[] target) throws IOException {
        checkClosed();
        checkAvailable(target.length);
        buffer.readBytes(target, 0, target.length);
    }

    @Override
    public void readFully(byte[] target, int offset, int length) throws IOException {
        checkClosed();
        checkAvailable(length);
        buffer.readBytes(target, offset, length);
    }

    @Override
    public boolean readBoolean() throws IOException {
        checkClosed();
        checkAvailable(Byte.BYTES);
        return buffer.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        checkClosed();
        checkAvailable(Byte.BYTES);
        return buffer.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        checkClosed();
        checkAvailable(Byte.BYTES);
        return buffer.readByte() & 0xff;
    }

    @Override
    public short readShort() throws IOException {
        checkClosed();
        checkAvailable(Short.BYTES);
        return buffer.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        checkClosed();
        checkAvailable(Short.BYTES);
        return buffer.readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException {
        checkClosed();
        checkAvailable(Short.BYTES);
        return (char) buffer.readShort();
    }

    @Override
    public int readInt() throws IOException {
        checkClosed();
        checkAvailable(Integer.BYTES);
        return buffer.readInt();
    }

    @Override
    public long readLong() throws IOException {
        checkClosed();
        checkAvailable(Long.BYTES);
        return buffer.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        checkClosed();
        checkAvailable(Float.BYTES);
        return buffer.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        checkClosed();
        checkAvailable(Double.BYTES);
        return buffer.readDouble();
    }

    private StringBuilder readBuffer;

    @Override
    public String readLine() throws IOException {
        checkClosed();
        int available = available();
        if (available == 0) {
            return null;
        }

        loop: do {
            final int c = buffer.readByte() & 0xff;
            --available;
            switch (c) {
                case '\n':
                    break loop;
                case '\r':
                    if (available > 0 && (char) buffer.getUnsignedByte(buffer.getReadOffset()) == '\n') {
                        buffer.advanceReadOffset(1);
                        --available;
                    }

                    break loop;
                default:
                    if (readBuffer == null) {
                        readBuffer = new StringBuilder();
                    }
                    readBuffer.append((char) c);
            }
        } while (available > 0);

        final String result = readBuffer != null && readBuffer.length() > 0 ? readBuffer.toString() : "";

        if (readBuffer != null) {
            readBuffer.setLength(0);
        }

        return result;
    }

    @Override
    public String readUTF() throws IOException {
        checkClosed();
        return DataInputStream.readUTF(this);
    }

    private void checkAvailable(int required) throws IOException {
        if (required < 0) {
            throw new IndexOutOfBoundsException("fieldSize cannot be a negative number");
        }

        if (required > available()) {
            throw new EOFException("The required number of bytes is too high! Length is " + required +
                                   ", but maximum readable is " + available());
        }
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("The ProtonBuffer InputStream has been closed");
        }
    }
}
