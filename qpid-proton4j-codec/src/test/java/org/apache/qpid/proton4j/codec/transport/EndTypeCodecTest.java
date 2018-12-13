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
package org.apache.qpid.proton4j.codec.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.End;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

public class EndTypeCodecTest extends CodecTestSupport {

    @Test
    public void testEncodeAndDecode() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        ErrorCondition error = new ErrorCondition();
        error.setCondition(Symbol.valueOf("amqp-error"));
        error.setDescription("Something bad");

        Map<Object, Object> infoMap = new LinkedHashMap<>();
        infoMap.put("1", true);
        infoMap.put("2", "string");

        error.setInfo(infoMap);

        End input = new End();
        input.setError(error);

        encoder.writeObject(buffer, encoderState, input);

        final End result = (End) decoder.readObject(buffer, decoderState);
        final ErrorCondition resultError = result.getError();

        assertNotNull(resultError);
        assertNotNull(resultError.getCondition());
        assertNotNull(resultError.getDescription());
        assertNotNull(resultError.getInfo());

        assertEquals(Symbol.valueOf("amqp-error"), resultError.getCondition());
        assertEquals("Something bad", resultError.getDescription());
        assertEquals(infoMap, resultError.getInfo());
    }
}
