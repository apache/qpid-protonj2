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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.messaging.Accepted;
import org.apache.qpid.proton4j.amqp.transactions.TransactionalState;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.codec.CodecTestSupport;
import org.junit.Test;

/**
 * Test for handling Declared serialization
 */
public class TransactionStateTypeCodeTest extends CodecTestSupport {

   @Test
   public void testEncodeDecodeType() throws Exception {
      ProtonBuffer buffer = ProtonByteBufferAllocator.DEFAULT.allocate();

      TransactionalState input = new TransactionalState();
      input.setTxnId(new Binary(new byte[] {2, 4, 6, 8}));
      input.setOutcome(Accepted.getInstance());

      encoder.writeObject(buffer, encoderState, input);

      final TransactionalState result = (TransactionalState)decoder.readObject(buffer, decoderState);

      assertSame(result.getOutcome(), Accepted.getInstance());

      assertNotNull(result.getTxnId());
      assertNotNull(result.getTxnId().getArray());

      assertArrayEquals(new byte[] {2, 4, 6, 8}, result.getTxnId().getArray());
   }
}
