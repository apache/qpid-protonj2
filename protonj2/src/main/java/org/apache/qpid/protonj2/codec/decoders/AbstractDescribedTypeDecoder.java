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

import java.io.InputStream;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.DecoderState;
import org.apache.qpid.protonj2.codec.DescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.StreamDecoderState;
import org.apache.qpid.protonj2.codec.StreamDescribedTypeDecoder;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;

/**
 * Abstract base for all Described Type decoders which implements the generic methods
 * common to all the implementations.
 *
 * @param <V> The type that this decoder handles.
 */
public abstract class AbstractDescribedTypeDecoder<V> implements DescribedTypeDecoder<V>, StreamDescribedTypeDecoder<V> {

    @Override
    public boolean isArrayType() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public String toString() {
        return "DescribedTypeDecoder<" + getTypeClass().getSimpleName() + ">";
    }

    @Override
    public int readSize(ProtonBuffer buffer, DecoderState state) {
        // Delegates to the inner described type which if it is another described type would
        // again delegate until a primitive AMQP type is reached.
        return state.getDecoder().readNextTypeDecoder(buffer, state).readSize(buffer, state);
    }

    @Override
    public int readSize(InputStream stream, StreamDecoderState state) {
        // Delegates to the inner described type which if it is another described type would
        // again delegate until a primitive AMQP type is reached.
        return state.getDecoder().readNextTypeDecoder(stream, state).readSize(stream, state);
    }

    @SuppressWarnings("unchecked")
    protected static <E> E checkIsExpectedTypeAndCast(Class<?> expected, TypeDecoder<?> actual) throws DecodeException {
        if (!expected.isAssignableFrom(actual.getClass())) {
            throw new DecodeException(
                "Expected " + expected + "encoding but got decoder for type: " + actual.getTypeClass().getName());
        }

        return (E) expected.cast(actual);
    }

    @SuppressWarnings("unchecked")
    protected static <E> E checkIsExpectedTypeAndCast(Class<?> expected, StreamTypeDecoder<?> actual) throws DecodeException {
        if (!expected.isAssignableFrom(actual.getClass())) {
            throw new DecodeException(
                "Expected " + expected + "encoding but got decoder for type: " + actual.getTypeClass().getName());
        }

        return (E) expected.cast(actual);
    }

    protected static void checkIsExpectedType(Class<?> expected, TypeDecoder<?> actual) throws DecodeException {
        if (!expected.isAssignableFrom(actual.getClass())) {
            throw new DecodeException(
                "Expected " + expected + "encoding but got decoder for type: " + actual.getTypeClass().getName());
        }
    }

    protected static void checkIsExpectedType(Class<?> expected, StreamTypeDecoder<?> actual) throws DecodeException {
        if (!expected.isAssignableFrom(actual.getClass())) {
            throw new DecodeException(
                "Expected " + expected + "encoding but got decoder for type: " + actual.getTypeClass().getName());
        }
    }
}
