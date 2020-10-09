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

import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;

/**
 * Interface for a TypeDecoder that manages decoding of AMQP primitive types.
 *
 * @param <V> the Type Class that this decoder manages.
 */
public interface PrimitiveTypeDecoder<V> extends TypeDecoder<V>, StreamTypeDecoder<V> {

    /**
     * @return true if the type managed by this decoder is assignable to a Java primitive type.
     */
    boolean isJavaPrimitive();

    /**
     * @return the AMQP Encoding Code that this primitive type decoder can read.
     */
    int getTypeCode();

}
