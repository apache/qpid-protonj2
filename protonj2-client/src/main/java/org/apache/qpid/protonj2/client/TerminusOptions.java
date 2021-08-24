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

import java.util.Arrays;

import org.apache.qpid.protonj2.types.UnsignedInteger;
import org.apache.qpid.protonj2.types.messaging.Source;
import org.apache.qpid.protonj2.types.messaging.Target;

/**
 * Base options type for configuration of {@link Source} and {@link Target} types
 * used by {@link Sender} and {@link Receiver} end points.
 *
 * @param <E> the subclass that implements this terminus options type.
 */
public abstract class TerminusOptions<E extends TerminusOptions<E>> {

    private DurabilityMode durabilityMode;
    private long timeout = -1;
    private ExpiryPolicy expiryPolicy;
    private String[] capabilities;

    abstract E self();

    /**
     * @return the durabilityMode
     */
    public DurabilityMode durabilityMode() {
        return durabilityMode;
    }

    /**
     * @param durabilityMode the durabilityMode to set
     *
     * @return this options instance.
     */
    public E durabilityMode(DurabilityMode durabilityMode) {
        this.durabilityMode = durabilityMode;
        return self();
    }

    /**
     * @return the timeout
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @param timeout the timeout to set
     *
     * @return this options instance.
     */
    public E timeout(long timeout) {
        if (timeout < 0 || timeout > UnsignedInteger.MAX_VALUE.longValue()) {
            throw new IllegalArgumentException("Timeout value must be in the range of an unsigned integer");
        }
        this.timeout = timeout;
        return self();
    }

    /**
     * @return the expiryPolicy
     */
    public ExpiryPolicy expiryPolicy() {
        return expiryPolicy;
    }

    /**
     * @param expiryPolicy the expiryPolicy to set
     *
     * @return this options instance.
     */
    public E expiryPolicy(ExpiryPolicy expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
        return self();
    }

    /**
     * @return the capabilities
     */
    public String[] capabilities() {
        return capabilities;
    }

    /**
     * @param capabilities the capabilities to set
     *
     * @return this options instance.
     */
    public E capabilities(String... capabilities) {
        this.capabilities = capabilities;
        return self();
    }

    protected void copyInto(TerminusOptions<E> other) {
        other.durabilityMode(durabilityMode);
        other.expiryPolicy(expiryPolicy);
        if (timeout > 0) {
            other.timeout(timeout);
        }
        if (capabilities != null) {
            other.capabilities(Arrays.copyOf(capabilities, capabilities.length));
        }
    }
}
