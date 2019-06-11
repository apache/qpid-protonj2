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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.transport.AmqpError;
import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.DetachTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transport.DetachTypeEncoder;
import org.junit.Test;

public class DetachTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Detach.class, new DetachTypeDecoder().getTypeClass());
        assertEquals(Detach.class, new DetachTypeEncoder().getTypeClass());
    }

    @Test
    public void testEncodeDecodeTypeWithNoError() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Detach input = new Detach();
        input.setHandle(1);
        input.setClosed(false);

        encoder.writeObject(buffer, encoderState, input);

        final Detach result = (Detach) decoder.readObject(buffer, decoderState);

        assertEquals(1, result.getHandle());
        assertFalse(result.getClosed());
        assertNull(result.getError());
    }

    @Test
    public void testEncodeDecodeTypeWithError() throws Exception {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
        ErrorCondition error = new ErrorCondition(Symbol.valueOf("amqp-error"), null);

        Detach input = new Detach();
        input.setHandle(1);
        input.setClosed(true);
        input.setError(error);

        encoder.writeObject(buffer, encoderState, input);

        final Detach result = (Detach) decoder.readObject(buffer, decoderState);

        assertEquals(1, result.getHandle());
        assertTrue(result.getClosed());
        assertNotNull(result.getError());
        assertNotNull(result.getError().getCondition());
        assertNull(result.getError().getDescription());
    }

    @Test
    public void testSkipValue() throws IOException {
        ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

        Detach detach = new Detach();

        detach.setError(new ErrorCondition(AmqpError.INVALID_FIELD, "test"));

        for (int i = 0; i < 10; ++i) {
            encoder.writeObject(buffer, encoderState, detach);
        }

        detach.setError(null);

        encoder.writeObject(buffer, encoderState, detach);

        for (int i = 0; i < 10; ++i) {
            TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
            assertEquals(Detach.class, typeDecoder.getTypeClass());
            typeDecoder.skipValue(buffer, decoderState);
        }

        final Object result = decoder.readObject(buffer, decoderState);

        assertNotNull(result);
        assertTrue(result instanceof Detach);

        Detach value = (Detach) result;
        assertNull(value.getError());
    }
}
