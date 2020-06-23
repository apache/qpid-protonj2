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
package org.apache.qpid.proton4j.test.driver.matchers.messaging;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

import java.util.Map;

import org.apache.qpid.proton4j.test.driver.codec.messaging.Source;
import org.apache.qpid.proton4j.test.driver.codec.messaging.TerminusDurability;
import org.apache.qpid.proton4j.test.driver.codec.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton4j.test.driver.codec.primitives.Symbol;
import org.apache.qpid.proton4j.test.driver.codec.primitives.UnsignedInteger;
import org.apache.qpid.proton4j.test.driver.codec.transport.DeliveryState;
import org.apache.qpid.proton4j.test.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.test.driver.matchers.ListDescribedTypeMatcher;
import org.hamcrest.Matcher;

public class SourceMatcher extends ListDescribedTypeMatcher {

    public SourceMatcher() {
        super(Source.Field.values().length, Source.DESCRIPTOR_CODE, Source.DESCRIPTOR_SYMBOL);
    }

    public SourceMatcher(Source source) {
        super(Source.Field.values().length, Source.DESCRIPTOR_CODE, Source.DESCRIPTOR_SYMBOL);

        addSourceMatchers(source);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Source.class;
    }

    //----- Type specific with methods that perform simple equals checks

    public SourceMatcher withAddress(String name) {
        return withAddress(equalTo(name));
    }

    public SourceMatcher withDurable(TerminusDurability durability) {
        return withDurable(equalTo(durability.getValue()));
    }

    public SourceMatcher withExpiryPolicy(TerminusExpiryPolicy expiry) {
        return withExpiryPolicy(equalTo(expiry.getPolicy()));
    }

    public SourceMatcher withTimeout(int timeout) {
        return withTimeout(equalTo(UnsignedInteger.valueOf(timeout)));
    }

    public SourceMatcher withTimeout(long timeout) {
        return withTimeout(equalTo(UnsignedInteger.valueOf(timeout)));
    }

    public SourceMatcher withTimeout(UnsignedInteger timeout) {
        return withTimeout(equalTo(timeout));
    }

    public SourceMatcher withDynamic(boolean dynamic) {
        return withDynamic(equalTo(dynamic));
    }

    public SourceMatcher withDynamicNodeProperties(Map<Symbol, Object> properties) {
        return withDynamicNodeProperties(equalTo(properties));
    }

    public SourceMatcher withDistributionMode(String distributionMode) {
        return withDistributionMode(equalTo(Symbol.valueOf(distributionMode)));
    }

    public SourceMatcher withDistributionMode(Symbol distributionMode) {
        return withDistributionMode(equalTo(distributionMode));
    }

    public SourceMatcher withFilter(Map<String, Object> filter) {
        return withFilter(equalTo(TypeMapper.toSymbolKeyedMap(filter)));
    }

    public SourceMatcher withFilterMap(Map<Symbol, Object> filter) {
        return withFilter(equalTo(filter));
    }

    public SourceMatcher withDefaultOutcome(DeliveryState defaultOutcome) {
        return withDefaultOutcome(equalTo(defaultOutcome));
    }

    public SourceMatcher withOutcomes(String... outcomes) {
        return withOutcomes(equalTo(TypeMapper.toSymbolArray(outcomes)));
    }

    public SourceMatcher withOutcomes(Symbol... outcomes) {
        return withOutcomes(equalTo(outcomes));
    }

    public SourceMatcher withCapabilities(String... capabilities) {
        return withCapabilities(equalTo(TypeMapper.toSymbolArray(capabilities)));
    }

    public SourceMatcher withCapabilities(Symbol... capabilities) {
        return withCapabilities(equalTo(capabilities));
    }

    //----- Matcher based with methods for more complex validation

    public SourceMatcher withAddress(Matcher<?> m) {
        addFieldMatcher(Source.Field.ADDRESS, m);
        return this;
    }

    public SourceMatcher withDurable(Matcher<?> m) {
        addFieldMatcher(Source.Field.DURABLE, m);
        return this;
    }

    public SourceMatcher withExpiryPolicy(Matcher<?> m) {
        addFieldMatcher(Source.Field.EXPIRY_POLICY, m);
        return this;
    }

    public SourceMatcher withTimeout(Matcher<?> m) {
        addFieldMatcher(Source.Field.TIMEOUT, m);
        return this;
    }

    public SourceMatcher withDynamic(Matcher<?> m) {
        addFieldMatcher(Source.Field.DYNAMIC, m);
        return this;
    }

    public SourceMatcher withDynamicNodeProperties(Matcher<?> m) {
        addFieldMatcher(Source.Field.DYNAMIC_NODE_PROPERTIES, m);
        return this;
    }

    public SourceMatcher withDistributionMode(Matcher<?> m) {
        addFieldMatcher(Source.Field.DISTRIBUTION_MODE, m);
        return this;
    }

    public SourceMatcher withFilter(Matcher<?> m) {
        addFieldMatcher(Source.Field.FILTER, m);
        return this;
    }

    public SourceMatcher withDefaultOutcome(Matcher<?> m) {
        addFieldMatcher(Source.Field.DEFAULT_OUTCOME, m);
        return this;
    }

    public SourceMatcher withOutcomes(Matcher<?> m) {
        addFieldMatcher(Source.Field.OUTCOMES, m);
        return this;
    }

    public SourceMatcher withCapabilities(Matcher<?> m) {
        addFieldMatcher(Source.Field.CAPABILITIES, m);
        return this;
    }

    //----- Populate the matcher from a given Source object

    private void addSourceMatchers(Source source) {
        if (source.getAddress() != null) {
            addFieldMatcher(Source.Field.ADDRESS, equalTo(source.getAddress()));
        } else {
            addFieldMatcher(Source.Field.ADDRESS, nullValue());
        }

        if (source.getDurable() != null) {
            addFieldMatcher(Source.Field.DURABLE, equalTo(source.getDurable()));
        } else {
            addFieldMatcher(Source.Field.DURABLE, nullValue());
        }

        if (source.getExpiryPolicy() != null) {
            addFieldMatcher(Source.Field.EXPIRY_POLICY, equalTo(source.getExpiryPolicy()));
        } else {
            addFieldMatcher(Source.Field.EXPIRY_POLICY, nullValue());
        }

        if (source.getTimeout() != null) {
            addFieldMatcher(Source.Field.TIMEOUT, equalTo(source.getTimeout()));
        } else {
            addFieldMatcher(Source.Field.TIMEOUT, nullValue());
        }

        addFieldMatcher(Source.Field.DYNAMIC, equalTo(source.getDynamic()));

        if (source.getDynamicNodeProperties() != null) {
            addFieldMatcher(Source.Field.DYNAMIC_NODE_PROPERTIES, equalTo(source.getDynamicNodeProperties()));
        } else {
            addFieldMatcher(Source.Field.DYNAMIC_NODE_PROPERTIES, nullValue());
        }

        if (source.getDistributionMode() != null) {
            addFieldMatcher(Source.Field.DISTRIBUTION_MODE, equalTo(source.getDistributionMode()));
        } else {
            addFieldMatcher(Source.Field.DISTRIBUTION_MODE, nullValue());
        }

        if (source.getFilter() != null) {
            addFieldMatcher(Source.Field.FILTER, equalTo(source.getFilter()));
        } else {
            addFieldMatcher(Source.Field.FILTER, nullValue());
        }

        if (source.getDefaultOutcome() != null) {
            addFieldMatcher(Source.Field.DEFAULT_OUTCOME, equalTo((DeliveryState) source.getDefaultOutcome()));
        } else {
            addFieldMatcher(Source.Field.DEFAULT_OUTCOME, nullValue());
        }

        if (source.getOutcomes() != null) {
            addFieldMatcher(Source.Field.OUTCOMES, equalTo(source.getOutcomes()));
        } else {
            addFieldMatcher(Source.Field.OUTCOMES, nullValue());
        }

        if (source.getCapabilities() != null) {
            addFieldMatcher(Source.Field.CAPABILITIES, equalTo(source.getCapabilities()));
        } else {
            addFieldMatcher(Source.Field.CAPABILITIES, nullValue());
        }
    }
}
