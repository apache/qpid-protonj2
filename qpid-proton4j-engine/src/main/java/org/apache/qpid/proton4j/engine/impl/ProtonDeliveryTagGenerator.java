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

import org.apache.qpid.proton4j.amqp.DeliveryTag;
import org.apache.qpid.proton4j.engine.DeliveryTagGenerator;

/**
 * Proton provided {@link DeliveryTagGenerator} utility.
 */
public abstract class ProtonDeliveryTagGenerator implements DeliveryTagGenerator {

    private static final ProtonEmptyTagGenerator EMPTY_TAG_GENERATOR = new ProtonEmptyTagGenerator();

    public enum BUILTIN {
        /**
         * Provides a {@link DeliveryTagGenerator} that creates tags based on an incrementing
         * numeric value starting from zero and moving upwards until the value wraps and continue
         * back towards zero.
         */
        SEQUENTIAL {

            @Override
            public DeliveryTagGenerator createGenerator() {
                return new ProtonSequentialTagGenerator();
            }
        },
        /**
         * Provides a {@link DeliveryTagGenerator} that creates tags based on a UUID value that
         * will be written as two long value encoded into the delivery tag bytes.
         */
        UUID {

            @Override
            public DeliveryTagGenerator createGenerator() {
                return new ProtonUuidTagGenerator();
            }
        },
        /**
         * Provides a {@link DeliveryTagGenerator} that uses a pool of {@link DeliveryTag} instances
         * in an attempt to reduce GC overhead on Delivery sends.  The tags are created using in numeric
         * base value that is incremented as new tag values are requested and none can be prodices from
         * the tag pool.
         */
        POOLED {

            @Override
            public DeliveryTagGenerator createGenerator() {
                return new ProtonPooledTagGenerator();
            }
        },
        /**
         * Provides a {@link DeliveryTagGenerator} that returns a singleton empty tag value that can be
         * used by senders that are sending settled deliveries and simply need to provide a non-null tag
         * value to the outgoing delivery instance.
         */
        EMPTY {

            @Override
            public DeliveryTagGenerator createGenerator() {
                return EMPTY_TAG_GENERATOR;
            }
        };

        public abstract DeliveryTagGenerator createGenerator();

    }

    private static final class ProtonEmptyTagGenerator implements DeliveryTagGenerator {

        private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};
        private static final DeliveryTag EMPTY_DELIVERY_TAG = new DeliveryTag.ProtonDeliveryTag(EMPTY_BYTE_ARRAY);

        @Override
        public DeliveryTag nextTag() {
            return EMPTY_DELIVERY_TAG;
        }
    }
}
