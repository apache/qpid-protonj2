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
package org.apache.qpid.proton4j.codec.encoders.primitives;

import org.apache.qpid.proton4j.codec.EncoderState;
import org.apache.qpid.proton4j.codec.PrimitiveArrayTypeEncoder;

import io.netty.buffer.ByteBuf;

/**
 * Encoder of AMQP Array types to a byte stream.
 */
public class ArrayTypeEncoder implements PrimitiveArrayTypeEncoder {

    @Override
    public void writeType(ByteBuf buffer, EncoderState state, Object value) {
        if (!value.getClass().isArray()) {
            throw new IllegalArgumentException("Expected Array type but got: " + value.getClass().getSimpleName());
        }

        Class<?> componentType = value.getClass().getComponentType();
        if (componentType.isPrimitive()) {
            if (componentType == Boolean.TYPE) {
                state.getEncoder().writeArray(buffer, state, (boolean[]) value);
            } else if (componentType == Byte.TYPE) {
                state.getEncoder().writeArray(buffer, state, (byte[]) value);
            } else if (componentType == Short.TYPE) {
                state.getEncoder().writeArray(buffer, state, (short[]) value);
            } else if (componentType == Integer.TYPE) {
                state.getEncoder().writeArray(buffer, state, (int[]) value);
            } else if (componentType == Long.TYPE) {
                state.getEncoder().writeArray(buffer, state, (long[]) value);
            } else if (componentType == Float.TYPE) {
                state.getEncoder().writeArray(buffer, state, (float[]) value);
            } else if (componentType == Double.TYPE) {
                state.getEncoder().writeArray(buffer, state, (double[]) value);
            } else if (componentType == Character.TYPE) {
                state.getEncoder().writeArray(buffer, state, (char[]) value);
            } else {
                throw new IllegalArgumentException(
                    "Cannot write arrays of type " + componentType.getName());
            }
        } else {
            writeArray(buffer, state, (Object[]) value);
        }
    }

    @Override
    public void writeValue(ByteBuf buffer, EncoderState state, Object value) {

        // TODO - Need to handle arrays of arrays
        // buffer.writeByte(EncodingCodes.ARRAY32);

    }

    //---- One Dimensional Optimized Array of Primitive Write methods --------//

    @Override
    public void writeArray(ByteBuf buffer, EncoderState state, Object[] value) {
    }

}
