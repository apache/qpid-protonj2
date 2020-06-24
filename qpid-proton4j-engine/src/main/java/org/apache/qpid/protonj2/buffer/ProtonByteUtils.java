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

/**
 * Set of Utility methods useful when dealing with byte arrays and other
 * primitive types.
 */
public abstract class ProtonByteUtils {

    public static byte[] toByteArray(byte value) {
        return writeByte(value, new byte[Byte.BYTES], 0);
    }

    public static byte[] toByteArray(short value) {
        return writeShort(value, new byte[Short.BYTES], 0);
    }

    public static byte[] toByteArray(int value) {
        return writeInt(value, new byte[Integer.BYTES], 0);
    }

    public static byte[] toByteArray(long value) {
        return writeLong(value, new byte[Long.BYTES], 0);
    }

    public static byte[] writeByte(byte value, byte[] destination, int offset) {
        destination[offset] = value;

        return destination;
    }

    public static byte[] writeShort(short value, byte[] destination, int offset) {
        destination[offset++] = (byte) (value >>> 8);
        destination[offset++] = (byte) (value >>> 0);

        return destination;
    }

    public static byte[] writeInt(int value, byte[] destination, int offset) {
        destination[offset++] = (byte) (value >>> 24);
        destination[offset++] = (byte) (value >>> 16);
        destination[offset++] = (byte) (value >>> 8);
        destination[offset++] = (byte) (value >>> 0);

        return destination;
    }

    public static byte[] writeLong(long value, byte[] destination, int offset) {
        destination[offset++] = (byte) (value >>> 56);
        destination[offset++] = (byte) (value >>> 48);
        destination[offset++] = (byte) (value >>> 40);
        destination[offset++] = (byte) (value >>> 32);
        destination[offset++] = (byte) (value >>> 24);
        destination[offset++] = (byte) (value >>> 16);
        destination[offset++] = (byte) (value >>> 8);
        destination[offset++] = (byte) (value >>> 0);

        return destination;
    }

    public static byte readByte(byte[] array, int offset) {
        return array[offset];
    }

    public static short readShort(byte[] array, int offset) {
        return (short) ((array[offset++] & 0xFF) << 8 |
                        (array[offset++] & 0xFF) << 0);
    }

    public static int readInt(byte[] array, int offset) {
        return (array[offset++] & 0xFF) << 24 |
               (array[offset++] & 0xFF) << 16 |
               (array[offset++] & 0xFF) << 8 |
               (array[offset++] & 0xFF) << 0;
    }

    public static long readLong(byte[] array, int offset) {
        return (long) (array[offset++] & 0xFF) << 56 |
               (long) (array[offset++] & 0xFF) << 48 |
               (long) (array[offset++] & 0xFF) << 40 |
               (long) (array[offset++] & 0xFF) << 32 |
               (long) (array[offset++] & 0xFF) << 24 |
               (long) (array[offset++] & 0xFF) << 16 |
               (long) (array[offset++] & 0xFF) << 8 |
               (long) (array[offset++] & 0xFF) << 0;
    }
}
