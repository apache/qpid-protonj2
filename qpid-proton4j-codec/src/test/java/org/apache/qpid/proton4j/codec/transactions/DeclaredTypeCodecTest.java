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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.transactions.Declared;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.DecodeException;
import org.apache.qpid.proton4j.codec.EncodingCodes;
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
   public void testEncodeDecodeTypeWithLargeResponseBlob() throws Exception {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       byte[] txnId = new byte[512];

       Random rand = new Random();
       rand.setSeed(System.currentTimeMillis());
       rand.nextBytes(txnId);

       Declared input = new Declared();

       input.setTxnId(new Binary(txnId));

       encoder.writeObject(buffer, encoderState, input);

       final Declared result = (Declared) decoder.readObject(buffer, decoderState);

       assertArrayEquals(txnId, result.getTxnId().getArray());
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

   @Test
   public void testSkipValueWithInvalidMap32Type() throws IOException {
       doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32);
   }

   @Test
   public void testSkipValueWithInvalidMap8Type() throws IOException {
       doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8);
   }

   private void doTestSkipValueWithInvalidMapType(byte mapType) throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       buffer.writeByte((byte) 0); // Described Type Indicator
       buffer.writeByte(EncodingCodes.SMALLULONG);
       buffer.writeByte(Declared.DESCRIPTOR_CODE.byteValue());
       if (mapType == EncodingCodes.MAP32) {
           buffer.writeByte(EncodingCodes.MAP32);
           buffer.writeInt((byte) 0);  // Size
           buffer.writeInt((byte) 0);  // Count
       } else {
           buffer.writeByte(EncodingCodes.MAP8);
           buffer.writeByte((byte) 0);  // Size
           buffer.writeByte((byte) 0);  // Count
       }

       TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
       assertEquals(Declared.class, typeDecoder.getTypeClass());

       try {
           typeDecoder.skipValue(buffer, decoderState);
           fail("Should not be able to skip type with invalid encoding");
       } catch (DecodeException ex) {}
   }

   @Test
   public void testDecodedWithInvalidMap32Type() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP32);
   }

   @Test
   public void testDecodeWithInvalidMap8Type() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP8);
   }

   private void doTestDecodeWithInvalidMapType(byte mapType) throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       buffer.writeByte((byte) 0); // Described Type Indicator
       buffer.writeByte(EncodingCodes.SMALLULONG);
       buffer.writeByte(Declared.DESCRIPTOR_CODE.byteValue());
       if (mapType == EncodingCodes.MAP32) {
           buffer.writeByte(EncodingCodes.MAP32);
           buffer.writeInt((byte) 0);  // Size
           buffer.writeInt((byte) 0);  // Count
       } else {
           buffer.writeByte(EncodingCodes.MAP8);
           buffer.writeByte((byte) 0);  // Size
           buffer.writeByte((byte) 0);  // Count
       }

       try {
           decoder.readObject(buffer, decoderState);
           fail("Should not decode type with invalid encoding");
       } catch (DecodeException ex) {}
   }

   @Test
   public void testEncodeDecodeArray() throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       Declared[] array = new Declared[3];

       array[0] = new Declared();
       array[1] = new Declared();
       array[2] = new Declared();

       array[0].setTxnId(new Binary(new byte[] {0}));
       array[1].setTxnId(new Binary(new byte[] {1}));
       array[2].setTxnId(new Binary(new byte[] {2}));

       encoder.writeObject(buffer, encoderState, array);

       final Object result = decoder.readObject(buffer, decoderState);

       assertTrue(result.getClass().isArray());
       assertEquals(Declared.class, result.getClass().getComponentType());

       Declared[] resultArray = (Declared[]) result;

       for (int i = 0; i < resultArray.length; ++i) {
           assertNotNull(resultArray[i]);
           assertTrue(resultArray[i] instanceof Declared);
           assertEquals(array[i].getTxnId(), resultArray[i].getTxnId());
       }
   }
}
