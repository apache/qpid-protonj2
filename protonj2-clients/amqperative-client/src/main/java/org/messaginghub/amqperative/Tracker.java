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

/**
 *
 */
public interface Tracker {

    Message<?> getMessage();
    //TODO: need this?

    /**
     * Gets the current remote state for the delivery.
     *
     * @return the remote delivery state
     */
    DeliveryState getRemoteState();

    /**
     * Gets whether the delivery was settled by the remote peer yet.
     *
     * @return whether the delivery is remotely settled
     */
    boolean isRemotelySettled();

    /**
     * Settles the delivery locally, if not {@link SenderOptions#isAutoSettle() auto-settling}.
     *
     * @return the delivery
     */
    Tracker settle();

    /**
     * Gets the delivery tag for this delivery
     *
     * @return the tag
     */
    byte[] getTag();

}
