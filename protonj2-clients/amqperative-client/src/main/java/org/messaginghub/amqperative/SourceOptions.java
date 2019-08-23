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

import java.util.HashMap;
import java.util.Map;

/**
 * Options type that carries configuration for link Source types.
 */
public final class SourceOptions extends TerminusOptions<SourceOptions> {

    /**
     * Control whether messages are browsed or consumed.
     */
    public enum DistributionMode {
        COPY,
        MOVE
    }

    private DistributionMode distributionMode;
    private Map<String, String> filters;

    public SourceOptions copyInto(SourceOptions other) {
        super.copyInto(other);
        other.setDistributionMode(distributionMode);
        if (filters != null) {
            other.setFilters(new HashMap<>(filters));
        }

        return this;
    }

    /**
     * @return the distributionMode
     */
    public DistributionMode getDistributionMode() {
        return distributionMode;
    }

    /**
     * @param distributionMode the distributionMode to set
     */
    public void setDistributionMode(DistributionMode distributionMode) {
        this.distributionMode = distributionMode;
    }

    /**
     * @return the filters
     */
    public Map<String, String> getFilters() {
        return filters;
    }

    /**
     * @param filters the filters to set
     */
    public void setFilters(Map<String, String> filters) {
        this.filters = filters;
    }

    @Override
    SourceOptions self() {
        return this;
    }
}
