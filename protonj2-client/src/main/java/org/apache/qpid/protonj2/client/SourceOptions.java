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
import java.util.HashMap;
import java.util.Map;

import org.apache.qpid.protonj2.client.impl.ClientDeliveryState;

/**
 * Options type that carries configuration for link Source types.
 */
public final class SourceOptions extends TerminusOptions<SourceOptions> {

    private static final DeliveryState.Type[] DEFAULT_OUTCOMES = new DeliveryState.Type[] {
        DeliveryState.Type.ACCEPTED, DeliveryState.Type.REJECTED, DeliveryState.Type.RELEASED, DeliveryState.Type.MODIFIED
    };

    public static final ClientDeliveryState DEFAULT_RECEIVER_OUTCOME = new ClientDeliveryState.ClientModified(true, false);

    private DistributionMode distributionMode;
    private DeliveryState defaultOutcome;
    private DeliveryState.Type[] outcomes = DEFAULT_OUTCOMES;
    private Map<String, String> filters;

    @Override
    public SourceOptions clone() {
        return copyInto(new SourceOptions());
    }

    public SourceOptions copyInto(SourceOptions other) {
        super.copyInto(other);
        other.distributionMode(distributionMode);
        if (filters != null) {
            other.filters(new HashMap<>(filters));
        }

        return this;
    }

    /**
     * @return the distributionMode
     */
    public DistributionMode distributionMode() {
        return distributionMode;
    }

    /**
     * @param distributionMode the distributionMode to set
     *
     * @return this {@link SourceOptions} instance.
     */
    public SourceOptions distributionMode(DistributionMode distributionMode) {
        this.distributionMode = distributionMode;
        return self();
    }

    /**
     * @return the filters
     */
    public Map<String, String> filters() {
        return filters;
    }

    /**
     * @param filters the filters to set
     *
     * @return this {@link SourceOptions} instance.
     */
    public SourceOptions filters(Map<String, String> filters) {
        this.filters = filters;
        return self();
    }

    /**
     * @return the configured default outcome as a {@link DeliveryState} instance.
     */
    public DeliveryState defaultOutcome() {
        return defaultOutcome;
    }

    /**
     * @param defaultOutcome
     * 		The default outcome to assign to the created link source.
     *
     * @return this {@link SourceOptions} instance.
     */
    public SourceOptions defaultOutcome(DeliveryState defaultOutcome) {
        this.defaultOutcome = defaultOutcome;
        return self();
    }

    /**
     * @return the currently configured supported outcomes to be used on the create link.
     */
    public DeliveryState.Type[] outcomes() {
        return outcomes;
    }

    /**
     * @param outcomes
     * 		The supported outcomes for the link created {@link Source}.
     *
     * @return this {@link SourceOptions} instance.
     */
    public SourceOptions outcomes(DeliveryState.Type... outcomes) {
        this.outcomes = outcomes != null ? Arrays.copyOf(outcomes, outcomes.length) : null;
        return self();
    }

    @Override
    SourceOptions self() {
        return this;
    }
}
