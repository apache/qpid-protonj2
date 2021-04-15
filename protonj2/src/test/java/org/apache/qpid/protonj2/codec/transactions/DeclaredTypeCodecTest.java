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
package org.apache.qpid.protonj2.codec.transactions;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.transactions.DeclaredTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.transactions.DeclaredTypeEncoder;
import org.apache.qpid.protonj2.types.Binary;
import org.apache.qpid.protonj2.types.transactions.Declared;
import org.junit.jupiter.api.Test;

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
       doTestEncodeDecodeType(false);
   }

   @Test
   public void testEncodeDecodeTypeFromStream() throws Exception {
       doTestEncodeDecodeType(true);
   }

   private void doTestEncodeDecodeType(boolean fromStream) throws Exception {
       final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       final InputStream stream = new ProtonBufferInputStream(buffer);

      Declared input = new Declared();
      input.setTxnId(new Binary(new byte[] {2, 4, 6, 8}));

      encoder.writeObject(buffer, encoderState, input);

      final Declared result;
      if (fromStream) {
          result = (Declared) streamDecoder.readObject(stream, streamDecoderState);
      } else {
          result = (Declared) decoder.readObject(buffer, decoderState);
      }

      assertNotNull(result.getTxnId());
      assertNotNull(result.getTxnId().getArray());

      assertArrayEquals(new byte[] {2, 4, 6, 8}, result.getTxnId().getArray());
   }

   @Test
   public void testEncodeDecodeTypeWithLargeResponseBlob() throws Exception {
       doTestEncodeDecodeTypeWithLargeResponseBlob(false);
   }

   @Test
   public void testEncodeDecodeTypeWithLargeResponseBlobFromStream() throws Exception {
       doTestEncodeDecodeTypeWithLargeResponseBlob(true);
   }

   private void doTestEncodeDecodeTypeWithLargeResponseBlob(boolean fromStream) throws Exception {
       final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       final InputStream stream = new ProtonBufferInputStream(buffer);

       byte[] txnId = new byte[512];

       Random rand = new Random();
       rand.setSeed(System.currentTimeMillis());
       rand.nextBytes(txnId);

       Declared input = new Declared();

       input.setTxnId(new Binary(txnId));

       encoder.writeObject(buffer, encoderState, input);

       final Declared result;
       if (fromStream) {
           result = (Declared) streamDecoder.readObject(stream, streamDecoderState);
       } else {
           result = (Declared) decoder.readObject(buffer, decoderState);
       }

       assertArrayEquals(txnId, result.getTxnId().getArray());
   }

   @Test
   public void testSkipValue() throws IOException {
       doTestSkipValue(false);
   }

   @Test
   public void testSkipValueFromStream() throws IOException {
       doTestSkipValue(true);
   }

   private void doTestSkipValue(boolean fromStream) throws IOException {
       final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       final InputStream stream = new ProtonBufferInputStream(buffer);

       Declared declared = new Declared();

       declared.setTxnId(new Binary(new byte[] {0}));

       for (int i = 0; i < 10; ++i) {
           encoder.writeObject(buffer, encoderState, declared);
       }

       declared.setTxnId(new Binary(new byte[] {1, 2}));

       encoder.writeObject(buffer, encoderState, declared);

       for (int i = 0; i < 10; ++i) {
           if (fromStream) {
               StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
               assertEquals(Declared.class, typeDecoder.getTypeClass());
               typeDecoder.skipValue(stream, streamDecoderState);
           } else {
               TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
               assertEquals(Declared.class, typeDecoder.getTypeClass());
               typeDecoder.skipValue(buffer, decoderState);
           }
       }

       final Declared result;
       if (fromStream) {
           result = (Declared) streamDecoder.readObject(stream, streamDecoderState);
       } else {
           result = (Declared) decoder.readObject(buffer, decoderState);
       }

       assertNotNull(result);
       assertTrue(result instanceof Declared);

       Declared value = result;
       assertArrayEquals(new byte[] {1, 2}, value.getTxnId().getArray());
   }

   @Test
   public void testSkipValueWithInvalidMap32Type() throws IOException {
       doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32, false);
   }

   @Test
   public void testSkipValueWithInvalidMap8Type() throws IOException {
       doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8, false);
   }

   @Test
   public void testSkipValueWithInvalidMap32TypeFromStream() throws IOException {
       doTestSkipValueWithInvalidMapType(EncodingCodes.MAP32, true);
   }

   @Test
   public void testSkipValueWithInvalidMap8TypeFromStream() throws IOException {
       doTestSkipValueWithInvalidMapType(EncodingCodes.MAP8, true);
   }

   private void doTestSkipValueWithInvalidMapType(byte mapType, boolean fromStream) throws IOException {
       final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       final InputStream stream = new ProtonBufferInputStream(buffer);

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

       if (fromStream) {
           StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
           assertEquals(Declared.class, typeDecoder.getTypeClass());

           try {
               typeDecoder.skipValue(stream, streamDecoderState);
               fail("Should not be able to skip type with invalid encoding");
           } catch (DecodeException ex) {}
       } else {
           TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
           assertEquals(Declared.class, typeDecoder.getTypeClass());

           try {
               typeDecoder.skipValue(buffer, decoderState);
               fail("Should not be able to skip type with invalid encoding");
           } catch (DecodeException ex) {}
       }
   }

   @Test
   public void testDecodedWithInvalidMap32Type() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, false);
   }

   @Test
   public void testDecodeWithInvalidMap8Type() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, false);
   }

   @Test
   public void testDecodedWithInvalidMap32TypeFromStream() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, true);
   }

   @Test
   public void testDecodeWithInvalidMap8TypeFromStream() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, true);
   }

   private void doTestDecodeWithInvalidMapType(byte mapType, boolean fromStream) throws IOException {
       final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       final InputStream stream = new ProtonBufferInputStream(buffer);

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

       if (fromStream) {
           try {
               streamDecoder.readObject(stream, streamDecoderState);
               fail("Should not decode type with invalid encoding");
           } catch (DecodeException ex) {}
       } else {
           try {
               decoder.readObject(buffer, decoderState);
               fail("Should not decode type with invalid encoding");
           } catch (DecodeException ex) {}
       }
   }

   @Test
   public void testEncodeDecodeArray() throws IOException {
       doTestEncodeDecodeArray(false);
   }

   @Test
   public void testEncodeDecodeArrayFromStream() throws IOException {
       doTestEncodeDecodeArray(true);
   }

   private void doTestEncodeDecodeArray(boolean fromStream) throws IOException {
       final ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       final InputStream stream = new ProtonBufferInputStream(buffer);

       Declared[] array = new Declared[3];

       array[0] = new Declared();
       array[1] = new Declared();
       array[2] = new Declared();

       array[0].setTxnId(new Binary(new byte[] {0}));
       array[1].setTxnId(new Binary(new byte[] {1}));
       array[2].setTxnId(new Binary(new byte[] {2}));

       encoder.writeObject(buffer, encoderState, array);

       final Object result;
       if (fromStream) {
           result = streamDecoder.readObject(stream, streamDecoderState);
       } else {
           result = decoder.readObject(buffer, decoderState);
       }

       assertTrue(result.getClass().isArray());
       assertEquals(Declared.class, result.getClass().getComponentType());

       Declared[] resultArray = (Declared[]) result;

       for (int i = 0; i < resultArray.length; ++i) {
           assertNotNull(resultArray[i]);
           assertTrue(resultArray[i] instanceof Declared);
           assertEquals(array[i].getTxnId(), resultArray[i].getTxnId());
       }
   }

   @Test
   public void testDecodeWithNotEnoughListEntriesList8() throws IOException {
       doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST8, false);
   }

   @Test
   public void testDecodeWithNotEnoughListEntriesList32() throws IOException {
       doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST32, false);
   }

   @Test
   public void testDecodeWithNotEnoughListEntriesList0FromStream() throws IOException {
       doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST0, true);
   }

   @Test
   public void testDecodeWithNotEnoughListEntriesList8FromStream() throws IOException {
       doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST8, true);
   }

   @Test
   public void testDecodeWithNotEnoughListEntriesList32FromStream() throws IOException {
       doTestDecodeWithNotEnoughListEntriesList32(EncodingCodes.LIST32, true);
   }

   private void doTestDecodeWithNotEnoughListEntriesList32(byte listType, boolean fromStream) throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       InputStream stream = new ProtonBufferInputStream(buffer);

       buffer.writeByte((byte) 0); // Described Type Indicator
       buffer.writeByte(EncodingCodes.SMALLULONG);
       buffer.writeByte(Declared.DESCRIPTOR_CODE.byteValue());
       if (listType == EncodingCodes.LIST32) {
           buffer.writeByte(EncodingCodes.LIST32);
           buffer.writeInt((byte) 0);  // Size
           buffer.writeInt((byte) 0);  // Count
       } else if (listType == EncodingCodes.LIST8) {
           buffer.writeByte(EncodingCodes.LIST8);
           buffer.writeByte((byte) 0);  // Size
           buffer.writeByte((byte) 0);  // Count
       } else {
           buffer.writeByte(EncodingCodes.LIST0);
       }

       if (fromStream) {
           try {
               streamDecoder.readObject(stream, streamDecoderState);
               fail("Should not decode type with invalid min entries");
           } catch (DecodeException ex) {}
       } else {
           try {
               decoder.readObject(buffer, decoderState);
               fail("Should not decode type with invalid min entries");
           } catch (DecodeException ex) {}
       }
   }

   @Test
   public void testDecodeWithToManyListEntriesList8() throws IOException {
       doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST8, false);
   }

   @Test
   public void testDecodeWithToManyListEntriesList32() throws IOException {
       doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST32, false);
   }

   @Test
   public void testDecodeWithToManyListEntriesList8FromStream() throws IOException {
       doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST8, true);
   }

   @Test
   public void testDecodeWithToManyListEntriesList32FromStream() throws IOException {
       doTestDecodeWithToManyListEntriesList32(EncodingCodes.LIST32, true);
   }

   private void doTestDecodeWithToManyListEntriesList32(byte listType, boolean fromStream) throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       InputStream stream = new ProtonBufferInputStream(buffer);

       buffer.writeByte((byte) 0); // Described Type Indicator
       buffer.writeByte(EncodingCodes.SMALLULONG);
       buffer.writeByte(Declared.DESCRIPTOR_CODE.byteValue());
       if (listType == EncodingCodes.LIST32) {
           buffer.writeByte(EncodingCodes.LIST32);
           buffer.writeInt(128);  // Size
           buffer.writeInt(127);  // Count
       } else if (listType == EncodingCodes.LIST8) {
           buffer.writeByte(EncodingCodes.LIST8);
           buffer.writeByte((byte) 128);  // Size
           buffer.writeByte((byte) 127);  // Count
       }

       if (fromStream) {
           try {
               streamDecoder.readObject(stream, streamDecoderState);
               fail("Should not decode type with invalid min entries");
           } catch (DecodeException ex) {}
       } else {
           try {
               decoder.readObject(buffer, decoderState);
               fail("Should not decode type with invalid min entries");
           } catch (DecodeException ex) {}
       }
   }
}
