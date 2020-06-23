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
package org.apache.qpid.proton4j.codec.decoders;

import java.lang.reflect.Array;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.DecoderState;

/**
 * Abstract base for all Described Type decoders which implements the generic methods
 * common to all the implementations.
 *
 * @param <V> The type that this primitive decoder handles.
 */
public abstract class AbstractPrimitiveTypeDecoder<V> implements PrimitiveTypeDecoder<V> {

    @Override
    public boolean isArrayType() {
        return false;
    }

    @Override
    public boolean isJavaPrimitive() {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V[] readArrayElements(ProtonBuffer buffer, DecoderState state, int count) throws DecodeException {
        V[] array = (V[]) Array.newInstance(getTypeClass(), count);
        for (int i = 0; i < count; ++i) {
            array[i] = readValue(buffer, state);
        }

        return array;
    }
}
