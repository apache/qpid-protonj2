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
package org.messaginghub.amqperative;

import java.util.Arrays;

import org.apache.qpid.proton4j.types.messaging.Source;
import org.apache.qpid.proton4j.types.messaging.Target;

/**
 * Base options type for configuration of {@link Source} and {@link Target} types
 * used by {@link Sender} and {@link Receiver} end points.
 *
 * @param <E> the subclass that implements this terminus options type.
 */
public abstract class TerminusOptions<E extends TerminusOptions<E>> {

    // TODO - Consider ease of use options vs exposing every possible AMQP
    //        configuration here to make the general users life simpler and
    //        expose some more low level proton / AMQP specific configuration
    //        that we can then assume the user knows what they are doing and
    //        not apply to much corrective logic.

    private DurabilityMode durabilityMode;
    private long timeout;
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
        other.timeout(timeout);
        other.expiryPolicy(expiryPolicy);
        if (capabilities != null) {
            other.capabilities(Arrays.copyOf(capabilities, capabilities.length));
        }
    }
}
