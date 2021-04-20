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
package org.apache.qpid.protonj2.codec.messaging;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferInputStream;
import org.apache.qpid.protonj2.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.protonj2.codec.CodecTestSupport;
import org.apache.qpid.protonj2.codec.DecodeException;
import org.apache.qpid.protonj2.codec.EncodingCodes;
import org.apache.qpid.protonj2.codec.StreamTypeDecoder;
import org.apache.qpid.protonj2.codec.TypeDecoder;
import org.apache.qpid.protonj2.codec.decoders.messaging.TargetTypeDecoder;
import org.apache.qpid.protonj2.codec.encoders.messaging.TargetTypeEncoder;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Modified;
import org.apache.qpid.protonj2.types.messaging.Target;
import org.apache.qpid.protonj2.types.messaging.TerminusDurability;
import org.apache.qpid.protonj2.types.messaging.TerminusExpiryPolicy;
import org.junit.jupiter.api.Test;

/**
 * Test for handling Source serialization
 */
public class TargetTypeCodeTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Target.class, new TargetTypeDecoder().getTypeClass());
        assertEquals(Target.class, new TargetTypeEncoder().getTypeClass());
    }

    @Test
    public void testDescriptors() throws IOException {
        assertEquals(Target.DESCRIPTOR_CODE, new TargetTypeDecoder().getDescriptorCode());
        assertEquals(Target.DESCRIPTOR_CODE, new TargetTypeEncoder().getDescriptorCode());
        assertEquals(Target.DESCRIPTOR_SYMBOL, new TargetTypeDecoder().getDescriptorSymbol());
        assertEquals(Target.DESCRIPTOR_SYMBOL, new TargetTypeEncoder().getDescriptorSymbol());
    }

   @Test
   public void testEncodeDecodeOfTarget() throws Exception {
       testEncodeDecodeOfTarget(false);
   }

   @Test
   public void testEncodeDecodeOfTargetFromStream() throws Exception {
       testEncodeDecodeOfTarget(true);
   }

   private void testEncodeDecodeOfTarget(boolean fromStream) throws Exception {
      ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
      InputStream stream = new ProtonBufferInputStream(buffer);

      Target value = new Target();
      value.setAddress("test");
      value.setDurable(TerminusDurability.UNSETTLED_STATE);
      value.setTimeout(UnsignedInteger.ZERO);

      encoder.writeObject(buffer, encoderState, value);

      final Target result;
      if (fromStream) {
          result = streamDecoder.readObject(stream, streamDecoderState, Target.class);
      } else {
          result = decoder.readObject(buffer, decoderState, Target.class);
      }

      assertEquals("test", result.getAddress());
      assertEquals(TerminusDurability.UNSETTLED_STATE, result.getDurable());
      assertEquals(UnsignedInteger.ZERO, result.getTimeout());
   }

   @Test
   public void testFullyPopulatedTarget() throws Exception {
       testFullyPopulatedTarget(false);
   }

   @Test
   public void testFullyPopulatedTargetFromStream() throws Exception {
       testFullyPopulatedTarget(true);
   }

   private void testFullyPopulatedTarget(boolean fromStream) throws Exception {
      ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
      InputStream stream = new ProtonBufferInputStream(buffer);

      Map<Symbol, Object> nodeProperties = new LinkedHashMap<>();
      nodeProperties.put(Symbol.valueOf("property-1"), "value-1");
      nodeProperties.put(Symbol.valueOf("property-2"), "value-2");
      nodeProperties.put(Symbol.valueOf("property-3"), "value-3");

      Target value = new Target();
      value.setAddress("test");
      value.setDurable(TerminusDurability.UNSETTLED_STATE);
      value.setExpiryPolicy(TerminusExpiryPolicy.CONNECTION_CLOSE);
      value.setTimeout(UnsignedInteger.valueOf(1024));
      value.setDynamic(false);
      value.setCapabilities(new Symbol[] {Symbol.valueOf("RELEASED"), Symbol.valueOf("MODIFIED")});
      value.setDynamicNodeProperties(nodeProperties);

      encoder.writeObject(buffer, encoderState, value);

      final Target result;
      if (fromStream) {
          result = streamDecoder.readObject(stream, streamDecoderState, Target.class);
      } else {
          result = decoder.readObject(buffer, decoderState, Target.class);
      }

      assertEquals("test", result.getAddress());
      assertEquals(TerminusDurability.UNSETTLED_STATE, result.getDurable());
      assertEquals(TerminusExpiryPolicy.CONNECTION_CLOSE, result.getExpiryPolicy());
      assertEquals(UnsignedInteger.valueOf(1024), result.getTimeout());
      assertEquals(false, result.isDynamic());
      assertEquals(nodeProperties, result.getDynamicNodeProperties());
      assertArrayEquals(new Symbol[] {Symbol.valueOf("RELEASED"), Symbol.valueOf("MODIFIED")}, result.getCapabilities());
   }

   @Test
   public void testSkipValue() throws IOException {
       testSkipValue(false);
   }

   @Test
   public void testSkipValueFromStream() throws IOException {
       testSkipValue(true);
   }

   private void testSkipValue(boolean fromStream) throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       InputStream stream = new ProtonBufferInputStream(buffer);

       Target target = new Target();
       target.setAddress("address");
       target.setDurable(TerminusDurability.CONFIGURATION);
       target.setCapabilities(Symbol.valueOf("QUEUE"));

       for (int i = 0; i < 10; ++i) {
           encoder.writeObject(buffer, encoderState, target);
       }

       encoder.writeObject(buffer, encoderState, new Modified());

       for (int i = 0; i < 10; ++i) {
           if (fromStream) {
               StreamTypeDecoder<?> typeDecoder = streamDecoder.readNextTypeDecoder(stream, streamDecoderState);
               assertEquals(Target.class, typeDecoder.getTypeClass());
               typeDecoder.skipValue(stream, streamDecoderState);
           } else {
               TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
               assertEquals(Target.class, typeDecoder.getTypeClass());
               typeDecoder.skipValue(buffer, decoderState);
           }
       }

       final Object result;
       if (fromStream) {
           result = streamDecoder.readObject(stream, streamDecoderState);
       } else {
           result = decoder.readObject(buffer, decoderState);
       }

       assertNotNull(result);
       assertTrue(result instanceof Modified);
       Modified modified = (Modified) result;
       assertFalse(modified.isUndeliverableHere());
       assertFalse(modified.isDeliveryFailed());
   }

   @Test
   public void testDecodeWithInvalidMap32Type() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, false);
   }

   @Test
   public void testDecodeWithInvalidMap8Type() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, false);
   }

   @Test
   public void testDecodeWithInvalidMap32TypeFromStream() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP32, true);
   }

   @Test
   public void testDecodeWithInvalidMap8TypeFromStream() throws IOException {
       doTestDecodeWithInvalidMapType(EncodingCodes.MAP8, true);
   }

   private void doTestDecodeWithInvalidMapType(byte mapType, boolean fromStream) throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       InputStream stream = new ProtonBufferInputStream(buffer);

       buffer.writeByte((byte) 0); // Described Type Indicator
       buffer.writeByte(EncodingCodes.SMALLULONG);
       buffer.writeByte(Target.DESCRIPTOR_CODE.byteValue());
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
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       InputStream stream = new ProtonBufferInputStream(buffer);

       buffer.writeByte((byte) 0); // Described Type Indicator
       buffer.writeByte(EncodingCodes.SMALLULONG);
       buffer.writeByte(Target.DESCRIPTOR_CODE.byteValue());
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
           assertEquals(Target.class, typeDecoder.getTypeClass());

           try {
               typeDecoder.skipValue(stream, streamDecoderState);
               fail("Should not be able to skip type with invalid encoding");
           } catch (DecodeException ex) {}
       } else {
           TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
           assertEquals(Target.class, typeDecoder.getTypeClass());

           try {
               typeDecoder.skipValue(buffer, decoderState);
               fail("Should not be able to skip type with invalid encoding");
           } catch (DecodeException ex) {}
       }
   }

   @Test
   public void testEncodeDecodeArray() throws IOException {
       testEncodeDecodeArray(false);
   }

   @Test
   public void testEncodeDecodeArrayFromStream() throws IOException {
       testEncodeDecodeArray(true);
   }

   private void testEncodeDecodeArray(boolean fromStream) throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();
       InputStream stream = new ProtonBufferInputStream(buffer);

       Target[] array = new Target[3];

       array[0] = new Target();
       array[1] = new Target();
       array[2] = new Target();

       array[0].setAddress("test-1").setDynamic(true).setDurable(TerminusDurability.CONFIGURATION);
       array[1].setAddress("test-2").setDynamic(true).setDurable(TerminusDurability.NONE);
       array[2].setAddress("test-3").setDynamic(true).setDurable(TerminusDurability.UNSETTLED_STATE);

       encoder.writeObject(buffer, encoderState, array);

       final Object result;
       if (fromStream) {
           result = streamDecoder.readObject(stream, streamDecoderState);
       } else {
           result = decoder.readObject(buffer, decoderState);
       }

       assertTrue(result.getClass().isArray());
       assertEquals(Target.class, result.getClass().getComponentType());

       Target[] resultArray = (Target[]) result;

       for (int i = 0; i < resultArray.length; ++i) {
           assertNotNull(resultArray[i]);
           assertTrue(resultArray[i] instanceof Target);
           assertEquals(array[i].getAddress(), resultArray[i].getAddress());
           assertEquals(array[i].isDynamic(), resultArray[i].isDynamic());
           assertEquals(array[i].getDurable(), resultArray[i].getDurable());
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
       buffer.writeByte(Target.DESCRIPTOR_CODE.byteValue());
       if (listType == EncodingCodes.LIST32) {
           buffer.writeByte(EncodingCodes.LIST32);
           buffer.writeInt(128);  // Size
           buffer.writeInt(-1);  // Count, reads as negative as encoder treats these as signed ints.
       } else if (listType == EncodingCodes.LIST8) {
           buffer.writeByte(EncodingCodes.LIST8);
           buffer.writeByte((byte) 128);  // Size
           buffer.writeByte((byte) 0xFF);  // Count
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
       buffer.writeByte(Target.DESCRIPTOR_CODE.byteValue());
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
