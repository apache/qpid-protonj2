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
package org.messaginghub.amqperative.util;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generator for Globally unique Strings used to identify resources within a given Connection.
 */
public class IdGenerator {

    private final String prefix;
    private final AtomicLong sequence = new AtomicLong(1);

    public static final String DEFAULT_PREFIX = "ID:";

    /**
     * Construct an IdGenerator using the given prefix value as the initial
     * prefix entry for all Ids generated (default is 'ID:').
     *
     * @param prefix
     *      The prefix value that is applied to all generated IDs.
     */
    public IdGenerator(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Construct an IdGenerator using the default prefix value.
     */
    public IdGenerator() {
        this(DEFAULT_PREFIX);
    }

    /**
     * Generate a unique id using the configured characteristics.
     *
     * @return a newly generated unique id value.
     */
    public String generateId() {
        StringBuilder sb = new StringBuilder(64);

        sb.append(prefix);
        sb.append(UUID.randomUUID());
        sb.append(":");
        sb.append(sequence.getAndIncrement());

        return sb.toString();
    }
}
