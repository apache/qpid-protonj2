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
package org.apache.qpid.proton4j.engine.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.engine.TransferTagGenerator;
import org.junit.Test;

public class ProtonCachingTagGeneratorTest {

    @Test
    public void testCreateTagGenerator() {
        TransferTagGenerator generator = ProtonTransferTagGenerator.BUILTIN.CACHING.createGenerator();
        assertTrue(generator instanceof ProtonCachingTagGenerator);
    }

    @Test
    public void testCreateTag() {
        ProtonCachingTagGenerator generator = new ProtonCachingTagGenerator();
        assertNotNull(generator.nextTag());
    }

    @Test
    public void testCreateTagsFromCacheAndReturn() {
        ProtonCachingTagGenerator generator = new ProtonCachingTagGenerator();

        final ArrayList<DeliveryTag> tags = new ArrayList<>(ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS);

        for (int i = 0; i < ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS; ++i) {
            tags.add(generator.nextTag());
        }

        tags.forEach(tag -> tag.release());

        for (int i = 0; i < ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS; ++i) {
            assertSame(tags.get(i), generator.nextTag());
        }

        DeliveryTag nonCached = generator.nextTag();
        assertFalse(tags.contains(nonCached));
        nonCached.release();
        assertFalse(tags.contains(nonCached));
    }

    @Test
    public void testConsumeAllCachedTagsAndThenReleaseAfterCreatingNonCached() {
        ProtonCachingTagGenerator generator = new ProtonCachingTagGenerator();

        DeliveryTag cachedTag = generator.nextTag();
        DeliveryTag nonCached = generator.nextTag();

        assertNotSame(cachedTag, nonCached);

        cachedTag.release();
        nonCached.release();

        DeliveryTag shouldBeCached = generator.nextTag();

        assertSame(cachedTag, shouldBeCached);
    }

    @Test
    public void testCreateTagsThatWrapAroundLimit() {
        ProtonCachingTagGenerator generator = new ProtonCachingTagGenerator();

        final ArrayList<DeliveryTag> tags = new ArrayList<>(ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS);

        for (int i = 0; i < ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS; ++i) {
            tags.add(generator.nextTag());
        }

        // Test that on wrap the tags start beyond the cached values.
        generator.setNextTagId(0xFFFFFFFFFFFFFFFFl);

        DeliveryTag maxUnsignedLong = generator.nextTag();
        DeliveryTag nextTagAfterWrap = generator.nextTag();

        assertEquals(Long.BYTES, maxUnsignedLong.tagBytes().length);
        assertEquals(Short.BYTES, nextTagAfterWrap.tagBytes().length);

        final short tagValue = getShort(nextTagAfterWrap.tagBytes());

        assertEquals(ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS, tagValue);

        tags.get(0).release();

        DeliveryTag tagAfterRelease = generator.nextTag();

        assertSame(tags.get(0), tagAfterRelease);
    }

    @Test
    public void testTakeAllTagsReturnThemAndTakeThemAgain() {
        ProtonCachingTagGenerator generator = new ProtonCachingTagGenerator();

        final ArrayList<DeliveryTag> tags1 = new ArrayList<>(ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS);
        final ArrayList<DeliveryTag> tags2 = new ArrayList<>(ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS);

        for (int i = 0; i < ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS; ++i) {
            tags1.add(generator.nextTag());
        }

        for (int i = 0; i < ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS; ++i) {
            tags1.get(i).release();
        }

        for (int i = 0; i < ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS; ++i) {
            tags2.add(generator.nextTag());
        }

        for (int i = 0; i < ProtonCachingTagGenerator.MAX_NUM_CACHED_TAGS; ++i) {
            assertSame(tags1.get(i), tags2.get(i));
        }
    }

    private short getShort(byte[] tagBytes) {
        return (short) ((tagBytes[0] & 0xFF) << 8 | (tagBytes[1] & 0xFF) << 0);
    }
}
