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
package org.apache.qpid.proton4j.amqp.driver.matchers.messaging;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

import org.apache.qpid.proton4j.amqp.driver.codec.messaging.Source;
import org.apache.qpid.proton4j.amqp.driver.codec.util.TypeMapper;
import org.apache.qpid.proton4j.amqp.driver.matchers.ListDescribedTypeMatcher;
import org.apache.qpid.proton4j.amqp.transport.DeliveryState;

public class SourceMatcher extends ListDescribedTypeMatcher {

    public SourceMatcher() {
        super(Source.Field.values().length, Source.DESCRIPTOR_CODE, Source.DESCRIPTOR_SYMBOL);
    }

    public SourceMatcher(org.apache.qpid.proton4j.amqp.messaging.Source source) {
        super(Source.Field.values().length, Source.DESCRIPTOR_CODE, Source.DESCRIPTOR_SYMBOL);

        addSourceMatchers(source);
    }

    @Override
    protected Class<?> getDescribedTypeClass() {
        return Source.class;
    }

    private void addSourceMatchers(org.apache.qpid.proton4j.amqp.messaging.Source source) {
        if (source.getAddress() != null) {
            addFieldMatcher(Source.Field.ADDRESS, equalTo(source.getAddress()));
        } else {
            addFieldMatcher(Source.Field.ADDRESS, nullValue());
        }

        if (source.getDurable() != null) {
            addFieldMatcher(Source.Field.DURABLE, equalTo(source.getDurable().getValue()));
        } else {
            addFieldMatcher(Source.Field.DURABLE, nullValue());
        }

        if (source.getExpiryPolicy() != null) {
            addFieldMatcher(Source.Field.EXPIRY_POLICY, equalTo(source.getExpiryPolicy().getPolicy()));
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
            addFieldMatcher(Source.Field.DEFAULT_OUTCOME, equalTo(
                TypeMapper.mapFromProtonType((DeliveryState) source.getDefaultOutcome())));
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
