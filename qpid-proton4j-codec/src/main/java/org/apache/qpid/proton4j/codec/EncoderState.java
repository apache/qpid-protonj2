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
 * Retains Encoder state information either between calls or across encode iterations.
 */
public interface EncoderState {

    /**
     * @return the Encoder instance that create this state object.
     */
    Encoder getEncoder();

    /**
     * Returns the bytes that represent an encoded Symbol.  The encoder can perform
     * caching or other optimization to make this translation faster.
     *
     * @param symbol
     *      The Symbol being encoded.
     *
     * @return the bytes that represent the Symbol.
     */
    byte[] getSymbolBytes(Symbol symbol);

}
