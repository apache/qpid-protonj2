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

import java.util.Map;

/**
 * Modified delivery state or outcome that carries details on the modification
 * provided by the remote peer or sent by the local client.
 */
public interface Modified extends DeliveryState {

    /**
     * @return <code>true</code> if the disposition indicates the delivery failed
     */
    boolean isDeliveryFailed();

    /**
     * @return <code>true</code> if the disposition indicates the delivery should not be attempted again at the target
     */
    boolean isUndeliverableHere();

    /**
     * @return a Map containing the annotations applied by the peer that created the disposition
     */
    Map<String, Object> getMessageAnnotations();

    @Override
    default Type getType() {
        return DeliveryState.Type.MODIFIED;
    }

    @Override
    default boolean isModified() {
        return true;
    }
}
