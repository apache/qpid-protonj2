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

import org.apache.qpid.proton.amqp.transport.Open;

/**
 * Factory for {@link LegacyTypeAdapter} instances
 */
public abstract class LegacyTypeAdapterFactory {

    public static LegacyTypeAdapter<?, ?> createTypeAdapter(Object legacyType) {

        if (legacyType instanceof Open) {
            return new LegacyOpenTypeAdapter((Open) legacyType);
        }

        throw new IllegalArgumentException("No type adapter available for given type");
    }
}
