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
package org.apache.qpid.proton4j.codec.legacy;

import org.hamcrest.TypeSafeMatcher;

/**
 * Base for Type Adapters from the legacy proton-j AMQP types to new types
 *
 * @param <L> The legacy type being adapted to
 * @param <N> The new type being adapted from
 */
public abstract class LegacyTypeAdapter<L, N> {

    protected final L legacyType;

    public LegacyTypeAdapter(L legacyType) {
        this.legacyType = legacyType;
    }

    /**
     * Create a {@link TypeSafeMatcher} for the legacy type to use when testing if new
     * AMQP types match legacy versions.
     *
     * @return a {@link TypeSafeMatcher} for the wrapped legacy proton-j type.
     */
    public abstract TypeSafeMatcher<N> createMatcher();

}
