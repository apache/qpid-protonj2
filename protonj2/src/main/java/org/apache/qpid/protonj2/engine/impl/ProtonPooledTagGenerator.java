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

import java.util.Queue;

import org.apache.qpid.protonj2.engine.util.RingQueue;
import org.apache.qpid.protonj2.types.DeliveryTag;

/**
 * Built in Transfer {@link DeliveryTag} generator that uses a fixed size tag
 * pool to reduce GC overhead by reusing tags that have been released from settled
 * messages.  When not using cached tags the generator creates new tags using a
 * running tag counter of type {@link Long} that assumes that when it wraps the user
 * has already release all tags within the lower range of the tag counter.
 */
public class ProtonPooledTagGenerator extends ProtonSequentialTagGenerator {

    /**
     * The default number of pooled transfer tags for this generator.
     */
    public static final int DEFAULT_MAX_NUM_POOLED_TAGS = 512;

    private final int tagPoolSize;
    private final Queue<ProtonPooledDeliveryTag> tagPool;

    /**
     * Creates a new {@link ProtonPooledTagGenerator} instance with the default pool size.
     */
    public ProtonPooledTagGenerator() {
        this(DEFAULT_MAX_NUM_POOLED_TAGS);
    }

    /**
     * Creates a new {@link ProtonPooledTagGenerator} instance with the given pool size.
     *
     * @param poolSize
     * 		The size of the transfer tag pool that should be allocated for this generator.
     */
    public ProtonPooledTagGenerator(int poolSize) {
        if (poolSize == 0) {
            throw new IllegalArgumentException("Cannot create a tag pool with zero pool size");
        }

        if (poolSize < 0) {
            throw new IllegalArgumentException("Cannot create a tag pool with negative pool size");
        }

        tagPoolSize = poolSize;
        tagPool = new RingQueue<>(tagPoolSize);
    }

    @Override
    public DeliveryTag nextTag() {
        ProtonPooledDeliveryTag nextTag = tagPool.poll();
        if (nextTag != null) {
            return nextTag.checkOut();
        } else {
            return createTag();
        }
    }

    private DeliveryTag createTag() {
        DeliveryTag nextTag = null;

        if (nextTagId >= 0 && nextTagId < tagPoolSize) {
            // Pooled tag that will return to pool on next release.
            nextTag = new ProtonPooledDeliveryTag((byte) nextTagId++).checkOut();
        } else {
            // Non-pooled tag that will not return to the pool on next release.
            nextTag = super.nextTag();
            if (nextTagId == 0) {
                nextTagId = tagPoolSize;
            }
        }

        return nextTag;
    }

    /*
     * Test entry point to validate tag pool and tag counter overflow.
     */
    @Override
    void setNextTagId(long nextIdValue) {
        this.nextTagId = nextIdValue;
    }

    //----- Specialized DeliveryTag and releases itself back to the pool

    private class ProtonPooledDeliveryTag extends ProtonNumericDeliveryTag {

        private boolean checkedOut;

        public ProtonPooledDeliveryTag(long tagValue) {
            super(tagValue);
        }

        public ProtonPooledDeliveryTag checkOut() {
            this.checkedOut = true;
            return this;
        }

        @Override
        public void release() {
            if (checkedOut) {
                tagPool.offer(this);
                checkedOut = false;
            }
        }
    }
}
