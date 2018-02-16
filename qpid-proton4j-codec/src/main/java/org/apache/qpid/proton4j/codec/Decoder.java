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
package org.apache.qpid.proton4j.codec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Decimal128;
import org.apache.qpid.proton4j.amqp.Decimal32;
import org.apache.qpid.proton4j.amqp.Decimal64;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.amqp.UnsignedShort;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Decode AMQP types from a byte stream
 */
public interface Decoder {

    DecoderState newDecoderState();

    Boolean readBoolean(ProtonBuffer buffer, DecoderState state) throws IOException;

    boolean readBoolean(ProtonBuffer buffer, DecoderState state, boolean defaultValue) throws IOException;

    Byte readByte(ProtonBuffer buffer, DecoderState state) throws IOException;

    byte readByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws IOException;

    UnsignedByte readUnsignedByte(ProtonBuffer buffer, DecoderState state) throws IOException;

    byte readUnsignedByte(ProtonBuffer buffer, DecoderState state, byte defaultValue) throws IOException;

    Character readCharacter(ProtonBuffer buffer, DecoderState state) throws IOException;

    char readCharacter(ProtonBuffer buffer, DecoderState state, char defaultValue) throws IOException;

    Decimal32 readDecimal32(ProtonBuffer buffer, DecoderState state) throws IOException;

    Decimal64 readDecimal64(ProtonBuffer buffer, DecoderState state) throws IOException;

    Decimal128 readDecimal128(ProtonBuffer buffer, DecoderState state) throws IOException;

    Short readShort(ProtonBuffer buffer, DecoderState state) throws IOException;

    short readShort(ProtonBuffer buffer, DecoderState state, short defaultValue) throws IOException;

    UnsignedShort readUnsignedShort(ProtonBuffer buffer, DecoderState state) throws IOException;

    int readUnsignedShort(ProtonBuffer buffer, DecoderState state, int defaultValue) throws IOException;

    Integer readInteger(ProtonBuffer buffer, DecoderState state) throws IOException;

    int readInteger(ProtonBuffer buffer, DecoderState state, int defaultValue) throws IOException;

    UnsignedInteger readUnsignedInteger(ProtonBuffer buffer, DecoderState state) throws IOException;

    long readUnsignedInteger(ProtonBuffer buffer, DecoderState state, long defaultValue) throws IOException;

    Long readLong(ProtonBuffer buffer, DecoderState state) throws IOException;

    long readLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws IOException;

    UnsignedLong readUnsignedLong(ProtonBuffer buffer, DecoderState state) throws IOException;

    // TODO - What do we do if the value is negative because it was to big?
    //        * Just force the user to deal with it
    //        * Omit this and require UnsignedLong
    //        * Some other option
    long readUnsignedLong(ProtonBuffer buffer, DecoderState state, long defaultValue) throws IOException;

    Float readFloat(ProtonBuffer buffer, DecoderState state) throws IOException;

    float readFloat(ProtonBuffer buffer, DecoderState state, float defaultValue) throws IOException;

    Double readDouble(ProtonBuffer buffer, DecoderState state) throws IOException;

    double readDouble(ProtonBuffer buffer, DecoderState state, double defaultValue) throws IOException;

    Binary readBinary(ProtonBuffer buffer, DecoderState state) throws IOException;

    String readString(ProtonBuffer buffer, DecoderState state) throws IOException;

    Symbol readSymbol(ProtonBuffer buffer, DecoderState state) throws IOException;

    String readSymbol(ProtonBuffer buffer, DecoderState state, String defaultValue) throws IOException;

    Long readTimestamp(ProtonBuffer buffer, DecoderState state) throws IOException;

    long readTimestamp(ProtonBuffer buffer, DecoderState state, long defaultValue) throws IOException;

    UUID readUUID(ProtonBuffer buffer, DecoderState state) throws IOException;

    Object readObject(ProtonBuffer buffer, DecoderState state) throws IOException;

    <T> T readObject(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws IOException;

    <T> T[] readMultiple(ProtonBuffer buffer, DecoderState state, final Class<T> clazz) throws IOException;

    <K,V> Map<K, V> readMap(ProtonBuffer buffer, DecoderState state) throws IOException;

    <V> List<V> readList(ProtonBuffer buffer, DecoderState state) throws IOException;

    TypeDecoder<?> readNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws IOException;

    TypeDecoder<?> peekNextTypeDecoder(ProtonBuffer buffer, DecoderState state) throws IOException;

    <V> Decoder registerTypeDecoder(TypeDecoder<V> decoder);

    TypeDecoder<?> getTypeDecoder(Object instance);

}
