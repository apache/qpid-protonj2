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
package org.apache.qpid.protonj2.client;

/**
 * Options class that controls various aspects of a {@link MessageOutputStream} instance.
 */
public class RawInputStreamOptions {

    /**
     * Creates a {@link RawInputStreamOptions} instance with default values for all options
     */
    public RawInputStreamOptions() {
    }

    /**
     * Create a {@link RawInputStreamOptions} instance that copies all configuration from the given
     * {@link RawInputStreamOptions} instance.
     *
     * @param options
     *      The options instance to copy all configuration values from.
     */
    public RawInputStreamOptions(RawInputStreamOptions options) {
        if (options != null) {
            options.copyInto(this);
        }
    }

    /**
     * Copy all options from this {@link RawInputStreamOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link RawInputStreamOptions} class for chaining.
     */
    protected RawInputStreamOptions copyInto(RawInputStreamOptions other) {
        return this;
    }
}