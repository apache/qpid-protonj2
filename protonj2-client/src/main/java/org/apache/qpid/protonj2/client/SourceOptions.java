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
import java.util.Objects;

import org.apache.qpid.protonj2.client.impl.ClientDeliveryState;
import org.apache.qpid.protonj2.types.DescribedType;

/**
 * Options type that carries configuration for link Source types.
 */
public final class SourceOptions extends TerminusOptions<SourceOptions> implements Cloneable {

    private static final DeliveryState.Type[] DEFAULT_OUTCOMES = new DeliveryState.Type[] {
        DeliveryState.Type.ACCEPTED, DeliveryState.Type.REJECTED, DeliveryState.Type.RELEASED, DeliveryState.Type.MODIFIED
    };

    /**
     * The default AMQP Outcome that will be specified for all new {@link Receiver} instances.
     */
    public static final ClientDeliveryState DEFAULT_RECEIVER_OUTCOME = new ClientDeliveryState.ClientModified(true, false);

    private DistributionMode distributionMode;
    private DeliveryState defaultOutcome;
    private DeliveryState.Type[] outcomes = DEFAULT_OUTCOMES;
    private Map<String, Object> filters;

    @Override
    public SourceOptions clone() {
        return copyInto(new SourceOptions());
    }

    /**
     * Copy all options from this {@link SourceOptions} instance into the instance
     * provided.
     *
     * @param other
     *      the target of this copy operation.
     *
     * @return this {@link SourceOptions} instance.
     */
    protected SourceOptions copyInto(SourceOptions other) {
        super.copyInto(other);
        other.distributionMode(distributionMode);
        if (filters != null) {
            other.filters(new HashMap<>(filters));
        }

        return other;
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
    public Map<String, Object> filters() {
        return filters;
    }

    /**
     * @param filters the filters to set
     *
     * @return this {@link SourceOptions} instance.
     */
    public SourceOptions filters(Map<String, Object> filters) {
        this.filters = filters;
        return self();
    }

    /**
     * Adds the given named filter into the map of filters (one will be created if not already set).
     * <p>
     * If a previous filters {@link Map} was assigned this new filter instance will be assigned
     * into that existing map, it is not cleared or reallocated. The descriptor should either be
     * an Symbol or UnsignedLong that aligns with the filters definition being used.
     *
     * @param name
     * 		The name to use when adding the described filter to the filters {@link Map}.
     * @param descriptor
     * 		The descriptor used for the {@link DescribedType} that will carry the filter.
     * @param filter
     * 		The filter value to assign to the filter {@link DescribedType}.
     *
     * @return this {@link SourceOptions} instance.
     */
    public SourceOptions addFilter(String name, Object descriptor, Object filter) {
        if (filters == null) {
            filters = new HashMap<>();
        }

        filters.put(name, new FilterDescribedType(descriptor, filter));

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

    private static class FilterDescribedType implements DescribedType {

        private final Object descriptor;
        private final Object described;

        public FilterDescribedType(Object descriptor, Object described) {
            this.descriptor = descriptor;
            this.described = described;
        }

        @Override
        public Object getDescriptor() {
            return descriptor;
        }

        @Override
        public Object getDescribed() {
            return this.described;
        }

        @Override
        public String toString() {
            return "FilterDescribedType{ descriptor:" + descriptor + ", described:" + described + " }";
        }

        @Override
        public int hashCode() {
            return Objects.hash(described, descriptor);
        }

        @Override
        public boolean equals(Object target) {
            if (this == target) {
                return true;
            }

            if (target == null) {
                return false;
            }

            if (!(target instanceof DescribedType)) {
                return false;
            }

            final DescribedType other = (DescribedType) target;

            return Objects.equals(descriptor, other.getDescriptor()) && Objects.equals(described, other.getDescribed());
        }
    }
}
