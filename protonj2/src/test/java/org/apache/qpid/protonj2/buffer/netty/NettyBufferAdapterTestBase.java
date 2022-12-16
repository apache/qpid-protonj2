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

package org.apache.qpid.protonj2.buffer.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.qpid.protonj2.buffer.ProtonAbstractBufferTest;
import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.buffer.ProtonBufferAllocator;
import org.junit.jupiter.api.Test;

/**
 * Base class with common tests for Netty to Proton buffer adapters
 */
public abstract class NettyBufferAdapterTestBase extends ProtonAbstractBufferTest {

    @Test
    public void testProtonBufferToNettyBufferWriteBytesFromLongs() {
        try (ProtonBufferAllocator proton = ProtonBufferAllocator.defaultAllocator();
             ProtonBufferAllocator netty = createTestCaseAllocator();
             ProtonBuffer protonBuffer = proton.allocate(16);
             ProtonBuffer nettyBuffer = netty.allocate(16)) {

            protonBuffer.writeLong(0x0102030405060708l);
            protonBuffer.writeLong(0x0102030405060708l);

            assertEquals(16, protonBuffer.getReadableBytes());

            nettyBuffer.writeBytes(protonBuffer);

            assertEquals(0, protonBuffer.getReadableBytes());
            assertEquals(16, nettyBuffer.getReadableBytes());

            assertEquals(0x0102030405060708l, nettyBuffer.readLong());
            assertEquals(0x0102030405060708l, nettyBuffer.readLong());
        }
    }

    @Test
    public void testProtonBufferToNettyBufferWriteBytesFromInts() {
        try (ProtonBufferAllocator proton = ProtonBufferAllocator.defaultAllocator();
             ProtonBufferAllocator netty = createTestCaseAllocator();
             ProtonBuffer protonBuffer = proton.allocate(16);
             ProtonBuffer nettyBuffer = netty.allocate(16)) {

            protonBuffer.writeInt(0x01020304);
            protonBuffer.writeInt(0x01020304);
            protonBuffer.writeInt(0x01020304);
            protonBuffer.writeInt(0x01020304);

            assertEquals(16, protonBuffer.getReadableBytes());

            nettyBuffer.writeBytes(protonBuffer);

            assertEquals(0, protonBuffer.getReadableBytes());
            assertEquals(16, nettyBuffer.getReadableBytes());

            assertEquals(0x01020304, nettyBuffer.readInt());
            assertEquals(0x01020304, nettyBuffer.readInt());
            assertEquals(0x01020304, nettyBuffer.readInt());
            assertEquals(0x01020304, nettyBuffer.readInt());
        }
    }

    @Test
    public void testProtonCompositeBufferToNettyBufferWriteBytesFromLongs() {
        try (ProtonBufferAllocator proton = ProtonBufferAllocator.defaultAllocator();
             ProtonBufferAllocator netty = createTestCaseAllocator();
             ProtonBuffer protonBuffer = proton.composite();
             ProtonBuffer nettyBuffer = netty.allocate(16)) {

            protonBuffer.ensureWritable(16);
            protonBuffer.writeLong(0x0102030405060708l);
            protonBuffer.writeLong(0x0102030405060708l);

            assertEquals(16, protonBuffer.getReadableBytes());

            nettyBuffer.writeBytes(protonBuffer);

            assertEquals(0, protonBuffer.getReadableBytes());
            assertEquals(16, nettyBuffer.getReadableBytes());

            assertEquals(0x0102030405060708l, nettyBuffer.readLong());
            assertEquals(0x0102030405060708l, nettyBuffer.readLong());
        }
    }

    @Test
    public void testProtonCompositeBufferToNettyBufferWriteBytesFromInts() {
        try (ProtonBufferAllocator proton = ProtonBufferAllocator.defaultAllocator();
             ProtonBufferAllocator netty = createTestCaseAllocator();
             ProtonBuffer protonBuffer = proton.composite();
             ProtonBuffer nettyBuffer = netty.allocate(16)) {

            protonBuffer.ensureWritable(16);
            protonBuffer.writeInt(0x01020304);
            protonBuffer.writeInt(0x01020304);
            protonBuffer.writeInt(0x01020304);
            protonBuffer.writeInt(0x01020304);

            assertEquals(16, protonBuffer.getReadableBytes());

            nettyBuffer.writeBytes(protonBuffer);

            assertEquals(0, protonBuffer.getReadableBytes());
            assertEquals(16, nettyBuffer.getReadableBytes());

            assertEquals(0x01020304, nettyBuffer.readInt());
            assertEquals(0x01020304, nettyBuffer.readInt());
            assertEquals(0x01020304, nettyBuffer.readInt());
            assertEquals(0x01020304, nettyBuffer.readInt());
        }
    }
}
