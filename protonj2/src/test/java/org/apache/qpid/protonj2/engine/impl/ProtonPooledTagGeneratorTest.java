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
package org.apache.qpid.protonj2.engine.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.apache.qpid.protonj2.engine.DeliveryTagGenerator;
import org.apache.qpid.protonj2.types.DeliveryTag;
import org.junit.Test;

public class ProtonPooledTagGeneratorTest {

    @Test
    public void testCreateTagGenerator() {
        DeliveryTagGenerator generator = ProtonDeliveryTagGenerator.BUILTIN.POOLED.createGenerator();
        assertTrue(generator instanceof ProtonPooledTagGenerator);
    }

    @Test
    public void testCreateTagGeneratorChecksPoolSze() {
        try {
            new ProtonPooledTagGenerator(0);
            fail("Should not allow non-pooling pool");
        } catch (IllegalArgumentException iae) {}

        try {
            new ProtonPooledTagGenerator(-1);
            fail("Should not allow negative sized pool");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testCreateTag() {
        ProtonPooledTagGenerator generator = new ProtonPooledTagGenerator();
        assertNotNull(generator.nextTag());
    }

    @Test
    public void testCreateTagsFromPoolAndReturn() {
        ProtonPooledTagGenerator generator = new ProtonPooledTagGenerator();

        final ArrayList<DeliveryTag> tags = new ArrayList<>(ProtonPooledTagGenerator.DEFAULT_MAX_NUM_POOLED_TAGS);

        for (int i = 0; i < ProtonPooledTagGenerator.DEFAULT_MAX_NUM_POOLED_TAGS; ++i) {
            tags.add(generator.nextTag());
        }

        tags.forEach(tag -> tag.release());

        for (int i = 0; i < ProtonPooledTagGenerator.DEFAULT_MAX_NUM_POOLED_TAGS; ++i) {
            assertSame(tags.get(i), generator.nextTag());
        }

        DeliveryTag nonCached = generator.nextTag();
        assertFalse(tags.contains(nonCached));
        nonCached.release();
        assertFalse(tags.contains(nonCached));
    }

    @Test
    public void testConsumeAllPooledTagsAndThenReleaseAfterCreatingNonPooled() {
        ProtonPooledTagGenerator generator = new ProtonPooledTagGenerator();

        DeliveryTag pooledTag = generator.nextTag();
        DeliveryTag nonCached = generator.nextTag();

        assertNotSame(pooledTag, nonCached);

        pooledTag.release();
        nonCached.release();

        DeliveryTag shouldBeCached = generator.nextTag();

        assertSame(pooledTag, shouldBeCached);
    }

    @Test
    public void testPooledTagReleaseIsIdempotent() {
        ProtonPooledTagGenerator generator = new ProtonPooledTagGenerator();

        DeliveryTag pooledTag = generator.nextTag();

        pooledTag.release();
        pooledTag.release();
        pooledTag.release();

        assertSame(pooledTag, generator.nextTag());
        assertNotSame(pooledTag, generator.nextTag());
        assertNotSame(pooledTag, generator.nextTag());
    }

    @Test
    public void testCreateTagsThatWrapAroundLimit() {
        ProtonPooledTagGenerator generator = new ProtonPooledTagGenerator();

        final ArrayList<DeliveryTag> tags = new ArrayList<>(ProtonPooledTagGenerator.DEFAULT_MAX_NUM_POOLED_TAGS);

        for (int i = 0; i < ProtonPooledTagGenerator.DEFAULT_MAX_NUM_POOLED_TAGS; ++i) {
            tags.add(generator.nextTag());
        }

        // Test that on wrap the tags start beyond the pooled values.
        generator.setNextTagId(0xFFFFFFFFFFFFFFFFl);

        DeliveryTag maxUnsignedLong = generator.nextTag();
        DeliveryTag nextTagAfterWrap = generator.nextTag();

        assertEquals(Long.BYTES, maxUnsignedLong.tagBytes().length);
        assertEquals(Short.BYTES, nextTagAfterWrap.tagBytes().length);

        final short tagValue = getShort(nextTagAfterWrap.tagBytes());

        assertEquals(ProtonPooledTagGenerator.DEFAULT_MAX_NUM_POOLED_TAGS, tagValue);

        tags.get(0).release();

        DeliveryTag tagAfterRelease = generator.nextTag();

        assertSame(tags.get(0), tagAfterRelease);
    }

    @Test
    public void testTakeAllTagsReturnThemAndTakeThemAgainDefaultSize() {
        doTestTakeAllTagsReturnThemAndTakeThemAgain(-1);
    }

    @Test
    public void testTakeAllTagsReturnThemAndTakeThemAgain() {
        doTestTakeAllTagsReturnThemAndTakeThemAgain(64);
    }

    private void doTestTakeAllTagsReturnThemAndTakeThemAgain(int poolSize) {
        final ProtonPooledTagGenerator generator;
        if (poolSize == -1) {
            generator = new ProtonPooledTagGenerator();
            poolSize = ProtonPooledTagGenerator.DEFAULT_MAX_NUM_POOLED_TAGS;
        } else {
            generator = new ProtonPooledTagGenerator(poolSize);
        }

        final ArrayList<DeliveryTag> tags1 = new ArrayList<>(poolSize);
        final ArrayList<DeliveryTag> tags2 = new ArrayList<>(poolSize);

        for (int i = 0; i < poolSize; ++i) {
            tags1.add(generator.nextTag());
        }

        for (int i = 0; i < poolSize; ++i) {
            tags1.get(i).release();
        }

        for (int i = 0; i < poolSize; ++i) {
            tags2.add(generator.nextTag());
        }

        for (int i = 0; i < poolSize; ++i) {
            assertSame(tags1.get(i), tags2.get(i));
        }
    }

    private short getShort(byte[] tagBytes) {
        return (short) ((tagBytes[0] & 0xFF) << 8 | (tagBytes[1] & 0xFF) << 0);
    }
}
