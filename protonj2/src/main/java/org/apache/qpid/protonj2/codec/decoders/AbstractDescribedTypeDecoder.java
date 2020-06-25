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

import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.TypeDecoder;

/**
 * Abstract base for all Described Type decoders which implements the generic methods
 * common to all the implementations.
 *
 * @param <V> The type that this decoder handles.
 */
public abstract class AbstractDescribedTypeDecoder<V> implements DescribedTypeDecoder<V> {

    @Override
    public boolean isArrayType() {
        return false;
    }

    @Override
    public String toString() {
        return "DescribedTypeDecoder<" + getTypeClass().getSimpleName() + ">";
    }

    protected static void checkIsExpectedType(Class<?> expected, TypeDecoder<?> actual) throws DecodeException {
        if (!expected.isAssignableFrom(actual.getClass())) {
            throw new DecodeException(
                "Expected " + expected + "encoding but got decoder for type: " + actual.getTypeClass().getName());
        }
    }
}
