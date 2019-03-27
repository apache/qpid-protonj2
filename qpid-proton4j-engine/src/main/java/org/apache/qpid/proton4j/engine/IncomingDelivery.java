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
package org.apache.qpid.proton4j.engine;

import org.apache.qpid.proton4j.amqp.transport.DeliveryState;

/**
 * API for an incoming Delivery.
 */
public interface IncomingDelivery extends Delivery {

    /**
     * @return the link that this {@link Delivery} is bound to.
     */
    @Override
    Receiver getLink();

    /**
     * Returns the number of bytes currently available for reading form this delivery, which may not be complete yet.
     *
     * Note that this value will change as bytes are received, and is in general not equal to the total length of
     * a delivery, except the point where {@link #isPartial()} returns false and no content has yet been received by
     * the application.
     *
     * @return the number of bytes currently available to read from this delivery.
     */
    int available();

    /**
     * Configures a default DeliveryState to be used if a received delivery is settled/freed
     * without any disposition state having been previously applied.
     *
     * @param state the default delivery state
     *
     * @return this delivery instance.
     */
    public IncomingDelivery setDefaultDeliveryState(DeliveryState state);

    /**
     * @return the default delivery state for this delivery
     */
    public DeliveryState getDefaultDeliveryState();

}
