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
package org.apache.qpid.proton4j.codec.messaging;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.messaging.Modified;
import org.apache.qpid.proton4j.amqp.messaging.Released;
import org.apache.qpid.proton4j.amqp.messaging.Source;
import org.apache.qpid.proton4j.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton4j.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.apache.qpid.proton4j.codec.TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.SourceTypeDecoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.SourceTypeEncoder;
import org.junit.Test;

/**
 * Test for handling Source serialization
 */
public class SourceTypeCodecTest extends CodecTestSupport {

    @Test
    public void testTypeClassReturnsCorrectType() throws IOException {
        assertEquals(Source.class, new SourceTypeDecoder().getTypeClass());
        assertEquals(Source.class, new SourceTypeEncoder().getTypeClass());
    }

   @Test
   public void testEncodeDecodeSourceType() throws Exception {
      ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

      Source value = new Source();
      value.setAddress("test");
      value.setDurable(TerminusDurability.UNSETTLED_STATE);

      encoder.writeObject(buffer, encoderState, value);

      final Source result = (Source)decoder.readObject(buffer, decoderState);

      assertEquals("test", result.getAddress());
      assertEquals(TerminusDurability.UNSETTLED_STATE, result.getDurable());
   }

   @Test
   public void testFullyPopulatedSource() throws Exception {
      ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

      Map<Symbol, Object> nodeProperties = new LinkedHashMap<>();
      nodeProperties.put(Symbol.valueOf("property-1"), "value-1");
      nodeProperties.put(Symbol.valueOf("property-2"), "value-2");
      nodeProperties.put(Symbol.valueOf("property-3"), "value-3");

      Map<Symbol, Object> filters = new LinkedHashMap<>();
      nodeProperties.put(Symbol.valueOf("filter-1"), "value-1");
      nodeProperties.put(Symbol.valueOf("filter-2"), "value-2");
      nodeProperties.put(Symbol.valueOf("filter-3"), "value-3");

      Source value = new Source();
      value.setAddress("test");
      value.setDurable(TerminusDurability.UNSETTLED_STATE);
      value.setExpiryPolicy(TerminusExpiryPolicy.SESSION_END);
      value.setTimeout(UnsignedInteger.valueOf(255));
      value.setDynamic(true);
      value.setDynamicNodeProperties(nodeProperties);
      value.setDistributionMode(Symbol.valueOf("mode"));
      value.setFilter(filters);
      value.setDefaultOutcome(Released.getInstance());
      value.setOutcomes(new Symbol[] {Symbol.valueOf("ACCEPTED"), Symbol.valueOf("REJECTED")});
      value.setCapabilities(new Symbol[] {Symbol.valueOf("RELEASED"), Symbol.valueOf("MODIFIED")});

      encoder.writeObject(buffer, encoderState, value);

      final Source result = (Source)decoder.readObject(buffer, decoderState);

      assertEquals("test", result.getAddress());
      assertEquals(TerminusDurability.UNSETTLED_STATE, result.getDurable());
      assertEquals(TerminusExpiryPolicy.SESSION_END, result.getExpiryPolicy());
      assertEquals(UnsignedInteger.valueOf(255), result.getTimeout());
      assertEquals(true, result.getDynamic());
      assertEquals(nodeProperties, result.getDynamicNodeProperties());
      assertEquals(Symbol.valueOf("mode"), result.getDistributionMode());
      assertEquals(filters, result.getFilter());
      assertEquals(Released.getInstance(), result.getDefaultOutcome());

      assertArrayEquals(new Symbol[] {Symbol.valueOf("ACCEPTED"), Symbol.valueOf("REJECTED")}, result.getOutcomes());
      assertArrayEquals(new Symbol[] {Symbol.valueOf("RELEASED"), Symbol.valueOf("MODIFIED")}, result.getCapabilities());
   }

   @Test
   public void testSkipValue() throws IOException {
       ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

       Source source = new Source();
       source.setAddress("address");
       source.setCapabilities(Symbol.valueOf("QUEUE"));

       for (int i = 0; i < 10; ++i) {
           encoder.writeObject(buffer, encoderState, source);
       }

       encoder.writeObject(buffer, encoderState, new Modified());

       for (int i = 0; i < 10; ++i) {
           TypeDecoder<?> typeDecoder = decoder.readNextTypeDecoder(buffer, decoderState);
           assertEquals(Source.class, typeDecoder.getTypeClass());
           typeDecoder.skipValue(buffer, decoderState);
       }

       final Object result = decoder.readObject(buffer, decoderState);

       assertNotNull(result);
       assertTrue(result instanceof Modified);
       Modified modified = (Modified) result;
       assertFalse(modified.getUndeliverableHere());
       assertFalse(modified.getDeliveryFailed());
   }
}
