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
package org.apache.qpid.proton4j.engine;

import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;

/**
 * Configuration options for the Engine
 */
public interface EngineConfiguration {

    /**
     * Sets the ProtonBufferAllocator used by this Engine.
     * <p>
     * When copying data, encoding types or otherwise needing to allocate memory
     * storage the Engine will use the assigned {@link ProtonBufferAllocator}.
     * If no allocator is assigned the Engine will use the default allocator.
     *
     * @param allocator
     *      The Allocator instance to use from this {@link Engine}.
     *
     * @return this {@link EngineConfiguration} for chaining.
     */
    EngineConfiguration setBufferAllocator(ProtonBufferAllocator allocator);

    /**
     * @return the currently assigned {@link ProtonBufferAllocator}.
     */
    ProtonBufferAllocator getBufferAllocator();

}
