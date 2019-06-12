/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.proton4j.codec.transactions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transactions.Declared;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DeclaredTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DeclaredTypeEncoder;
import org.junit.Test;

/**
 * Test for handling Declared serialization
 */
public class DeclaredTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Declared.class, new DeclaredTypeEncoder().getTypeClass());
        assertEquals(Declared.class, new DeclaredTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws Exception {
        DeclaredTypeDecoder decoder = new DeclaredTypeDecoder();
        DeclaredTypeEncoder encoder = new DeclaredTypeEncoder();

        assertEquals(Declared.DESCRIPTOR_CODE, decoder.getDescriptorCode());
        assertEquals(Declared.DESCRIPTOR_CODE, encoder.getDescriptorCode());
        assertEquals(Declared.DESCRIPTOR_SYMBOL, decoder.getDescriptorSymbol());
        assertEquals(Declared.DESCRIPTOR_SYMBOL, encoder.getDescriptorSymbol());
    }

   @Test
   public void testEncodeDecodeType() throws Exception {
      ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

      Declared input = new Declared();
      input.setTxnId(new Binary(new byte[] {2, 4, 6, 8}));

      encoder.writeObject(buffer, encoderState, input);

      final Declared result = (Declared)decoder.readObject(buffer, decoderState);

      assertNotNull(result.getTxnId());
      assertNotNull(result.getTxnId().getArray());

      assertArrayEquals(new byte[] {2, 4, 6, 8}, result.getTxnId().getArray());
   }

   @Test
   public void testSkipValue() throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       Declared declared = new Declared();

       declared.setTxnId(new Binary(new byte[] {0}));

       for (int i = 0; i < 10; ++i) {
           encoder.writeObject(buffer, encoderState, declared);
       }

       declared.setTxnId(new Binary(new byte[] {1, 2}));

       encoder.writeObject(buffer, encoderState, declared);

       for (int i = 0; i < 10; ++i) {
           TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
           assertEquals(Declared.class, typeDecoder.getTypeClass());
           typeDecoder.skipValue(buffer, decoderState);
       }

       final Object result = decoder.readObject(buffer, decoderState);

       assertNotNull(result);
       assertTrue(result instanceof Declared);

       Declared value = (Declared) result;
       assertArrayEquals(new byte[] {1, 2}, value.getTxnId().getArray());
   }
}
