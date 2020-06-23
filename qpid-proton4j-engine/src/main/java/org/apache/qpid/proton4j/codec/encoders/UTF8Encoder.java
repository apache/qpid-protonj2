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
package org.apache.qpid.proton4j.codec.encoders;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Interface for an external UTF8 Encoder that can be supplied by a client
 * which implements custom encoding logic optimized for the application using
 * the Codec.
 */
public interface UTF8Encoder {

    /**
     * Encodes the given sequence of characters in UTF8 to the given buffer.
     *
     * @param buffer
     *      A ProtonBuffer where the UTF-8 encoded bytes should be written.
     * @param sequence
     *      A {@link CharSequence} representing the UTF-8 bytes to encode
     *
     * @return a reference to the encoding buffer for chaining
     */
    ProtonBuffer encodeUTF8(ProtonBuffer buffer, CharSequence sequence);

}
