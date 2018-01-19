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

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.codec.DecoderState;

/**
 * State object used by the Built in Decoder implementation.
 */
public class ProtonDecoderState implements DecoderState {

    private final CharsetDecoder STRING_DECODER = StandardCharsets.UTF_8.newDecoder();

    private final ProtonDecoder decoder;

    private UTF8Decoder stringDecoder;

    public ProtonDecoderState(ProtonDecoder decoder) {
        this.decoder = decoder;
    }

    @Override
    public ProtonDecoder getDecoder() {
        return decoder;
    }

    public UTF8Decoder getStringDecoder() {
        return stringDecoder;
    }

    public void setStringDecoder(UTF8Decoder stringDecoder) {
        this.stringDecoder = stringDecoder;
    }

    @Override
    public String decodeUTF8(ProtonBuffer utf8bytes) {
        try {
            if (stringDecoder == null) {
                return STRING_DECODER.decode(utf8bytes.toByteBuffer()).toString();
            } else {
                return stringDecoder.decodeUTF8(utf8bytes);
            }
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Cannot parse encoded UTF8 String");
        } finally {
            STRING_DECODER.reset();
        }
    }
}
