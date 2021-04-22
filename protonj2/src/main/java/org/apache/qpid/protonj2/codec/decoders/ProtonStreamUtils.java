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

import org.apache.qpid.protonj2.codec.DecodeEOFException;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodeException;

/**
 * Set of Utility methods useful when dealing with byte arrays and other
 * primitive types.
 */
public abstract class ProtonStreamUtils {

    private static final byte[] EMPTY_ARRAY = new byte[0];

    /**
     * Write the given {@link Byte} to the target {@link OutputStream}.
     *
     * @param value
     * 		the value to write to the {@link OutputStream}.
     * @param stream
     * 		the {@link OutputStream} where the target value is to be written.
     *
     * @return the given {@link OutputStream} instance.
     *
     * @throws EncodeException if an error occurs while writing to the target {@link OutputStream}.
     */
    public static OutputStream writeByte(byte value, OutputStream stream) throws EncodeException {
        try {
            stream.write(value);
        } catch (IOException ex) {
            throw new DecodeException("Caught IO error writing to provided stream", ex);
        }

        return stream;
    }

    /**
     * Write the given {@link Short} to the target {@link OutputStream}.
     *
     * @param value
     * 		the value to write to the {@link OutputStream}.
     * @param stream
     * 		the {@link OutputStream} where the target value is to be written.
     *
     * @return the given {@link OutputStream} instance.
     *
     * @throws EncodeException if an error occurs while writing to the target {@link OutputStream}.
     */
    public static OutputStream writeShort(short value, OutputStream stream) throws EncodeException {
        writeByte((byte) (value >>> 8), stream);
        writeByte((byte) (value >>> 0), stream);

        return stream;
    }

    /**
     * Write the given {@link Integer} to the target {@link OutputStream}.
     *
     * @param value
     * 		the value to write to the {@link OutputStream}.
     * @param stream
     * 		the {@link OutputStream} where the target value is to be written.
     *
     * @return the given {@link OutputStream} instance.
     *
     * @throws EncodeException if an error occurs while writing to the target {@link OutputStream}.
     */
    public static OutputStream writeInt(int value, OutputStream stream) throws EncodeException {
        writeByte((byte) (value >>> 24), stream);
        writeByte((byte) (value >>> 16), stream);
        writeByte((byte) (value >>> 8), stream);
        writeByte((byte) (value >>> 0), stream);

        return stream;
    }

    /**
     * Write the given {@link Long} to the target {@link OutputStream}.
     *
     * @param value
     * 		the value to write to the {@link OutputStream}.
     * @param stream
     * 		the {@link OutputStream} where the target value is to be written.
     *
     * @return the given {@link OutputStream} instance.
     *
     * @throws EncodeException if an error occurs while writing to the target {@link OutputStream}.
     */
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

    /**
     * Reads the given number of bytes from the provided {@link InputStream} into an array and
     * return that to the caller.  If the requested number of bytes cannot be read from the stream
     * an {@link DecodeException} is thrown to indicate an underflow.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     * @param length
     * 		The number of bytes to read from the given input stream.
     *
     * @return a byte array containing the requested number of bytes read from the given {@link InputStream}
     *
     * @throws DecodeException if an error occurs reading from the stream or insufficient bytes are available.
     */
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

    /**
     * Reads a single byte from the given {@link InputStream} and thrown a {@link DecodeException} if the
     * {@link InputStream} indicates an EOF condition was encountered.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     *
     * @return the given byte that was read from the stream.
     *
     * @throws DecodeException if an error occurs during the read or EOF is reached.
     */
    public static byte readEncodingCode(InputStream stream) throws DecodeException {
        try {
            int result = stream.read();
            if (result >= 0) {
                return (byte) result;
            } else {
                throw new DecodeEOFException("Cannot read more type information from stream that has reached its end.");
            }
        } catch (IOException ex) {
            throw new DecodeException("Caught IO error reading from provided stream", ex);
        }
    }

    /**
     * Reads a single byte from the given {@link InputStream} and thrown a {@link DecodeException} if the
     * {@link InputStream} indicates an EOF condition was encountered.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     *
     * @return the given byte that was read from the stream.
     *
     * @throws DecodeException if an error occurs during the read or EOF is reached.
     */
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

    /**
     * Reads a short value from the given {@link InputStream} and thrown a {@link DecodeException} if the
     * {@link InputStream} indicates an EOF condition was encountered.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     *
     * @return the given byte that was read from the stream.
     *
     * @throws DecodeException if an error occurs during the read or EOF is reached.
     */
    public static short readShort(InputStream stream) {
        return (short) ((readByte(stream) & 0xFF) << 8 |
                        (readByte(stream) & 0xFF) << 0);
    }

    /**
     * Reads a integer value from the given {@link InputStream} and thrown a {@link DecodeException} if the
     * {@link InputStream} indicates an EOF condition was encountered.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     *
     * @return the given byte that was read from the stream.
     *
     * @throws DecodeException if an error occurs during the read or EOF is reached.
     */
    public static int readInt(InputStream stream) {
        return (readByte(stream) & 0xFF) << 24 |
               (readByte(stream) & 0xFF) << 16 |
               (readByte(stream) & 0xFF) << 8 |
               (readByte(stream) & 0xFF) << 0;
    }

    /**
     * Reads a long value from the given {@link InputStream} and thrown a {@link DecodeException} if the
     * {@link InputStream} indicates an EOF condition was encountered.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     *
     * @return the given byte that was read from the stream.
     *
     * @throws DecodeException if an error occurs during the read or EOF is reached.
     */
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

    /**
     * Reads a float value from the given {@link InputStream} and thrown a {@link DecodeException} if the
     * {@link InputStream} indicates an EOF condition was encountered.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     *
     * @return the given byte that was read from the stream.
     *
     * @throws DecodeException if an error occurs during the read or EOF is reached.
     */
    public static float readFloat(InputStream stream) {
        return Float.intBitsToFloat(readInt(stream));
    }

    /**
     * Reads a double value from the given {@link InputStream} and thrown a {@link DecodeException} if the
     * {@link InputStream} indicates an EOF condition was encountered.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     *
     * @return the given byte that was read from the stream.
     *
     * @throws DecodeException if an error occurs during the read or EOF is reached.
     */
    public static double readDouble(InputStream stream) {
        return Double.longBitsToDouble(readLong(stream));
    }

    /**
     * Attempts to skip the given number of bytes from the provided {@link InputStream} instance and
     * throws a DecodeException if an error occurs during the skip.
     *
     * @param stream
     * 		The {@link InputStream} where the bytes should be read from.
     * @param amount
     *      The number of bytes that should be skipped.
     *
     * @return the {@link InputStream} instance that was passed.
     *
     * @throws DecodeException if an error occurs during the read or EOF is reached.
     */
    public static InputStream skipBytes(InputStream stream, long amount) {
        try {
            stream.skip(amount);
        } catch (IOException ex) {
            throw new DecodeException(
                String.format("Error while attempting to skip %d bytes in the given InputStream", amount), ex);
        }

        return stream;
    }

    /**
     * Attempts to reset the provided {@link InputStream} to a previously marked point.  If an error occurs
     * this method throws an DecodeException to describe the error.
     *
     * @param stream
     * 		The {@link InputStream} that is to be reset.
     *
     * @return the {@link InputStream} instance that was passed.
     *
     * @throws DecodeException if an error occurs during the reset.
     */
    public static InputStream reset(InputStream stream) throws DecodeException {
        try {
            stream.reset();
        } catch (IOException ex) {
            throw new DecodeException("Caught IO error when calling reset on provided stream", ex);
        }

        return stream;
    }
}
