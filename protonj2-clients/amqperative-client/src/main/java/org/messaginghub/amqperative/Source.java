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

import java.util.Map;
import java.util.Set;

/**
 * The Source for messages.
 *
 * For an opened {@link Sender} or {@link Receiver} the Source properties exposes the
 * remote {@link Source} configuration.
 */
public interface Source {

    /**
     * @return the address of the Source node.
     */
    String address();

    /**
     * @return the durabilityMode of this Source node.
     */
    DurabilityMode durabilityMode();

    /**
     * @return the timeout assigned to this Source node in seconds.
     */
    long timeout();

    /**
     * @return the {@link ExpiryPolicy} of this Source node.
     */
    ExpiryPolicy expiryPolicy();

    /**
     * @return true if the Source node dynamically on-demand
     */
    boolean dynamic();

    /**
     * @return the properties of the dynamically created Source node.
     */
    Map<String, Object> dynamicNodeProperties();

    /**
     * @return the {@link DistributionMode} of this Source node.
     */
    DistributionMode distributionMode();

    /**
     * @return the filters assigned to this Source node.
     */
    Map<String, String> filters();

    /**
     * @return the default outcome configured for this Source node.
     */
    DeliveryState defaultOutcome();

    /**
     * @return the supported outcome types of this Source node.
     */
    Set<DeliveryState.Type> outcomes();

    /**
     * @return the set of capabilities available on this Source node.
     */
    Set<String> capabilities();

}
