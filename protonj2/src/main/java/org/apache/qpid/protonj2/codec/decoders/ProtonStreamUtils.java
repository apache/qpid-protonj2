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
package org.apache.qpid.protonj2.codec.decoders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodeException;

/**
 * Set of Utility methods useful when dealing with byte arrays and other
 * primitive types.
 */
public abstract class ProtonStreamUtils {

    private static final byte[] EMPTY_ARRAY = new byte[0];

    public static OutputStream writeByte(byte value, OutputStream stream) throws EncodeException {
        try {
            stream.write(value);
        } catch (IOException ex) {
            throw new DecodeException("Caught IO error writing to provided stream", ex);
        }

        return stream;
    }

    public static OutputStream writeShort(short value, OutputStream stream) throws EncodeException {
        writeByte((byte) (value >>> 8), stream);
        writeByte((byte) (value >>> 0), stream);

        return stream;
    }

    public static OutputStream writeInt(int value, OutputStream stream) throws EncodeException {
        writeByte((byte) (value >>> 24), stream);
        writeByte((byte) (value >>> 16), stream);
        writeByte((byte) (value >>> 8), stream);
        writeByte((byte) (value >>> 0), stream);

        return stream;
    }

    public static OutputStream writeLong(long value, OutputStream stream) throws EncodeException {
        writeByte((byte) (value >>> 56), stream);
        writeByte((byte) (value >>> 48), stream);
        writeByte((byte) (value >>> 40), stream);
        writeByte((byte) (value >>> 32), stream);
        writeByte((byte) (value >>> 24), stream);
        writeByte((byte) (value >>> 16), stream);
        writeByte((byte) (value >>> 8), stream);
        writeByte((byte) (value >>> 0), stream);

        return stream;
    }

    public static byte[] readBytes(InputStream stream, int length) throws DecodeException {
        try {
            if (length == 0) {
                return EMPTY_ARRAY;
            } else {
                final byte[] payload = new byte[length];

                // NOTE: In JDK 11 we could use stream.readNBytes(length) which would allow
                //       the stream to more efficiently provide the resulting bytes possibly
                //       without a memory copy.

                if (stream.read(payload) < length) {
                    throw new DecodeException(String.format(
                        "Failed to read requested number of bytes %d: instead only %d bytes were read.", length, payload.length));
                }

                return payload;
            }
        } catch (IOException ex) {
            throw new DecodeException("Caught IO error reading from provided stream", ex);
        }
    }

    public static byte readByte(InputStream stream) throws DecodeException {
        try {
            int result = stream.read();
            if (result >= 0) {
                return (byte) result;
            } else {
                throw new DecodeException("Unexpectedly reached the end of the provided stream");
            }
        } catch (IOException ex) {
            throw new DecodeException("Caught IO error reading from provided stream", ex);
        }
    }

    public static short readShort(InputStream stream) {
        return (short) ((readByte(stream) & 0xFF) << 8 |
                        (readByte(stream) & 0xFF) << 0);
    }

    public static int readInt(InputStream stream) {
        return (readByte(stream) & 0xFF) << 24 |
               (readByte(stream) & 0xFF) << 16 |
               (readByte(stream) & 0xFF) << 8 |
               (readByte(stream) & 0xFF) << 0;
    }

    public static long readLong(InputStream stream) {
        return (long) (readByte(stream) & 0xFF) << 56 |
               (long) (readByte(stream) & 0xFF) << 48 |
               (long) (readByte(stream) & 0xFF) << 40 |
               (long) (readByte(stream) & 0xFF) << 32 |
               (long) (readByte(stream) & 0xFF) << 24 |
               (long) (readByte(stream) & 0xFF) << 16 |
               (long) (readByte(stream) & 0xFF) << 8 |
               (long) (readByte(stream) & 0xFF) << 0;
    }

    public static float readFloat(InputStream stream) {
        return Float.intBitsToFloat(readInt(stream));
    }

    public static double readDouble(InputStream stream) {
        return Double.longBitsToDouble(readLong(stream));
    }

    public static InputStream skipBytes(InputStream stream, long amount) {
        try {
            stream.skip(amount);
        } catch (IOException ex) {
            throw new DecodeException(
                String.format("Error while attempting to skip %d bytes in the given InputStream", amount), ex);
        }

        return stream;
    }

    public static void reset(InputStream stream) throws DecodeException {
        try {
            stream.reset();
        } catch (IOException ex) {
            throw new DecodeException("Caught IO error when calling reset on provided stream", ex);
        }
    }
}
