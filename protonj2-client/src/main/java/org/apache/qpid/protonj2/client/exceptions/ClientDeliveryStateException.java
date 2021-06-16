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
package org.apache.qpid.protonj2.client.exceptions;

import org.apache.qpid.protonj2.client.Delivery;
import org.apache.qpid.protonj2.client.DeliveryState;
import org.apache.qpid.protonj2.client.Tracker;
import org.apache.qpid.protonj2.types.messaging.Accepted;
import org.apache.qpid.protonj2.types.messaging.Rejected;

/**
 * Thrown from client API that deal with a {@link Delivery} or {@link Tracker} where the outcome
 * that results from that API can affect whether the API call succeeded or failed.  Such a case might
 * be that a sent message is awaiting a remote {@link Accepted} outcome but instead the remote sends
 * a {@link Rejected} outcome.
 */
public class ClientDeliveryStateException extends ClientIllegalStateException {

    private static final long serialVersionUID = -4699002536747966516L;

    private final DeliveryState outcome;

    /**
     * Create a new instance of the client delivery state error.
     *
     * @param message
     * 		The message that describes the cause of the error.
     * @param outcome
     * 		The {@link DeliveryState} that caused the error.
     */
    public ClientDeliveryStateException(String message, DeliveryState outcome) {
        super(message);
        this.outcome = outcome;
    }

    /**
     * Create a new instance of the client delivery state error.
     *
     * @param message
     * 		The message that describes the cause of the error.
     * @param cause
     * 		The exception that initially triggered this error.
     * @param outcome
     * 		The {@link DeliveryState} that caused the error.
     */
    public ClientDeliveryStateException(String message, Throwable cause, DeliveryState outcome) {
        super(message, cause);
        this.outcome = outcome;
    }

    /**
     * @return the {@link DeliveryState} that defines the outcome returned from the remote peer.
     */
    public DeliveryState getOutcome() {
        return this.outcome;
    }
}
