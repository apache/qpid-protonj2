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
package org.apache.qpid.protonj2.client.impl;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.DistributionMode;
import org.apache.qpid.protonj2.client.DurabilityMode;
import org.apache.qpid.protonj2.client.ExpiryPolicy;
import org.apache.qpid.protonj2.client.Source;
import org.apache.qpid.protonj2.types.Symbol;

/**
 * Wrapper around a remote {@link Source} that provides read-only access to
 * the remote Source configuration.
 */
final class ClientRemoteSource implements Source {

    private final org.apache.qpid.protonj2.types.messaging.Source remoteSource;

    private DeliveryState cachedDefaultOutcome;
    private DistributionMode cachedDistributionMode;
    private Map<String, Object> cachedDynamicNodeProperties;
    private Map<String, String> cachedFilters;
    private Set<DeliveryState.Type> cachedOutcomes;
    private Set<String> cachedCapabilities;

    ClientRemoteSource(org.apache.qpid.protonj2.types.messaging.Source remoteSource) {
        this.remoteSource = remoteSource;
    }

    @Override
    public String address() {
        return remoteSource.getAddress();
    }

    @Override
    public DurabilityMode durabilityMode() {
        if (remoteSource.getDurable() != null) {
            switch (remoteSource.getDurable()) {
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
        return remoteSource.getTimeout() == null ? 0 : remoteSource.getTimeout().longValue();
    }

    @Override
    public ExpiryPolicy expiryPolicy() {
        if (remoteSource.getExpiryPolicy() != null) {
            switch (remoteSource.getExpiryPolicy()) {
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
        return remoteSource.isDynamic();
    }

    @Override
    public Map<String, Object> dynamicNodeProperties() {
        if (cachedDynamicNodeProperties == null && remoteSource.getDynamicNodeProperties() != null) {
            cachedDynamicNodeProperties =
                Collections.unmodifiableMap(ClientConversionSupport.toStringKeyedMap(remoteSource.getDynamicNodeProperties()));
        }

        return cachedDynamicNodeProperties;
    }

    @Override
    public DistributionMode distributionMode() {
        if (cachedDistributionMode == null && remoteSource.getDistributionMode() != null) {
            switch (remoteSource.getDistributionMode().toString()) {
                case "MOVE":
                    cachedDistributionMode = DistributionMode.MOVE;
                    break;
                case "COPY":
                    cachedDistributionMode = DistributionMode.COPY;
                    break;
                default:
                    break;
            }
        }

        return cachedDistributionMode;
    }

    @Override
    public Map<String, String> filters() {
        if (cachedFilters == null && remoteSource.getFilter() != null) {
            final Map<String, String> result = cachedFilters = new LinkedHashMap<>();
            remoteSource.getFilter().forEach((key, value) -> {
                result.put(key.toString(), value.toString());
            });
        }

        return cachedFilters;
    }

    @Override
    public DeliveryState defaultOutcome() {
        if (cachedDefaultOutcome == null && remoteSource.getDefaultOutcome() != null) {
            cachedDefaultOutcome = ClientDeliveryState.fromProtonType(remoteSource.getDefaultOutcome());
        }

        return cachedDefaultOutcome;
    }

    @Override
    public Set<DeliveryState.Type> outcomes() {
        if (cachedOutcomes == null && remoteSource.getOutcomes() != null) {
            cachedOutcomes = new LinkedHashSet<>(remoteSource.getOutcomes().length);
            for (Symbol outcomeName : remoteSource.getOutcomes()) {
                cachedOutcomes.add(ClientDeliveryState.fromOutcomeSymbol(outcomeName));
            }

            cachedOutcomes = Collections.unmodifiableSet(cachedOutcomes);
        }

        return cachedOutcomes;
    }

    @Override
    public Set<String> capabilities() {
        if (cachedCapabilities == null && remoteSource.getCapabilities() != null) {
            cachedCapabilities = Collections.unmodifiableSet(ClientConversionSupport.toStringSet(remoteSource.getCapabilities()));
        }

        return cachedCapabilities;
    }
}
