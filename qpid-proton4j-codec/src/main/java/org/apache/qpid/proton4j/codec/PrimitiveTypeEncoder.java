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

import io.netty.buffer.ByteBuf;

/**
 * Encoder of Primitive types such as Integer or Boolean
 *
 * @param <V> the type that this encoder writes.
 */
public interface PrimitiveTypeEncoder<V> extends TypeEncoder<V> {

    default boolean isArryTypeEncoder() {
        return false;
    }

    /**
     * Write an array of values to the given byte buffer.
     * <p>
     * This method writes an single dimension array of values to the
     * given byte buffer including the AMQP array type constructor that
     * matches the encoded form of the array.
     *
     * @param buffer
     *      The buffer to write the AMQP type to
     * @param state
     *      The current encoder state
     * @param value
     *      The array of values that is to be written.
     */
    void writeArray(ByteBuf buffer, EncoderState state, V[] value);

}
