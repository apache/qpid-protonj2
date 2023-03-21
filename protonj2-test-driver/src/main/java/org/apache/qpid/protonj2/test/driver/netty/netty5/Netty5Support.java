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

package org.apache.qpid.protonj2.test.driver.netty.netty5;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty5.buffer.BufferAllocator;

/**
 * Support class used to detect if Netty 5 is available on the class path.
 */
public final class Netty5Support {

    private static final Logger LOG = LoggerFactory.getLogger(Netty5Support.class);

    private static final Throwable UNAVAILABILITY_CAUSE;
    static {
        Throwable cause = null;
        try {
            BufferAllocator.onHeapUnpooled();
        } catch (Throwable ex) {
            LOG.debug("Netty 5 not available for use.");
            cause = ex;
        }

        UNAVAILABILITY_CAUSE = cause;
    }

    public static final boolean isAvailable() {
        return UNAVAILABILITY_CAUSE == null;
    }
}
