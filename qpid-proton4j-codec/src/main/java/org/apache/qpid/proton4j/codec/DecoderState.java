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

import org.apache.qpid.proton4j.amqp.Symbol;

/**
 * Retains state of decode either between calls or across decode iterations
 */
public interface DecoderState {

    /**
     * @return the decoder that created this state object
     */
    Decoder getDecoder();

    /**
     * Returns a Symbol instance using the bytes that represent the
     * encoded Symbol.  The Decoder can optimize this using caching or
     * other techniques to reduce GC and String encode / decode overhead.
     *
     * @param symbolBytes
     *      The ASCII bytes that make up the Symbol value.
     *
     * @return a Symbol instance that represents the given bytes
     */
    Symbol getSymbol(byte[] symbolBytes);

}
