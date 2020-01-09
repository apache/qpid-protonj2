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
package org.messaginghub.amqperative.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.messaginghub.amqperative.DurabilityMode;
import org.messaginghub.amqperative.ExpiryPolicy;
import org.messaginghub.amqperative.Target;

/**
 * Wrapper around a remote {@link Target} that provides read-only access to
 * the remote Target configuration.
 */
public class RemoteTarget implements Target {

    private final org.apache.qpid.proton4j.amqp.messaging.Target remoteTarget;

    private Map<String, Object> cachedDynamicNodeProperties;
    private Set<String> cachedCapabilities;

    RemoteTarget(org.apache.qpid.proton4j.amqp.messaging.Target remoteTarget) {
        this.remoteTarget = remoteTarget;
    }

    @Override
    public String address() {
        return remoteTarget.getAddress();
    }

    @Override
    public DurabilityMode durabilityMode() {
        if (remoteTarget.getDurable() != null) {
            switch (remoteTarget.getDurable()) {
                case NONE:
                    return DurabilityMode.NONE;
                case CONFIGURATION:
                    return DurabilityMode.CONFIGURATION;
                case UNSETTLED_STATE:
                    return DurabilityMode.UNSETTLED_STATE;
            }
        }

        return DurabilityMode.NONE;
    }

    @Override
    public long timeout() {
        return remoteTarget.getTimeout() == null ? 0 : remoteTarget.getTimeout().longValue();
    }

    @Override
    public ExpiryPolicy expiryPolicy() {
        if (remoteTarget.getExpiryPolicy() != null) {
            switch (remoteTarget.getExpiryPolicy()) {
            case LINK_DETACH:
                return ExpiryPolicy.LINK_CLOSE;
            case SESSION_END:
                return ExpiryPolicy.SESSION_CLOSE;
            case CONNECTION_CLOSE:
                return ExpiryPolicy.CONNECTION_CLOSE;
            case NEVER:
                return ExpiryPolicy.NEVER;
            }
        }

        return ExpiryPolicy.SESSION_CLOSE;
    }

    @Override
    public boolean dynamic() {
        return remoteTarget.isDynamic();
    }

    @Override
    public Map<String, Object> dynamicNodeProperties() {
        if (cachedDynamicNodeProperties == null && remoteTarget.getDynamicNodeProperties() != null) {
            cachedDynamicNodeProperties =
                Collections.unmodifiableMap(ClientConversionSupport.toStringKeyedMap(remoteTarget.getDynamicNodeProperties()));
        }

        return cachedDynamicNodeProperties;
    }

    @Override
    public Set<String> capabilities() {
        if (cachedCapabilities == null && remoteTarget.getCapabilities() != null) {
            cachedCapabilities = Collections.unmodifiableSet(ClientConversionSupport.toStringSet(remoteTarget.getCapabilities()));
        }

        return cachedCapabilities;
    }
}
