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

/**
 * Transactional {@link DeliveryState} marker interface used when a {@link Delivery}
 * or {@link Tracker} is enlisted in a transaction. The transactional delivery state
 * carries an outcome that indicates the eventual outcome applied to the associated
 * AMQP delivery once the transaction is successfully discharged.
 * <p>
 * The API of the transactional delivery-state outcomes will return <code>true</code>
 * to indicate the type of outcome {@link Accepted}, {@link Rejected}, {@link Released}
 * or {@link Modified}. To inspect the actual assigned outcomes the {@link #getOutcome()}
 * method should be called and the resulting {@link DeliveryState} instance inspected.
 */
public interface Transactional extends DeliveryState {

    /**
     * @return the outcome that will be applied to the delivery once the transaction is successfully discharged
     */
    DeliveryState getOutcome();

    @Override
    default Type getType() {
        return DeliveryState.Type.TRANSACTIONAL;
    }

    @Override
    default boolean isTransactional() {
        return true;
    }
}
