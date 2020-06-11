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
package org.apache.qpid.proton4j.types.messaging;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;
import org.apache.qpid.proton4j.types.transport.DeliveryState;
import org.apache.qpid.proton4j.types.transport.ErrorCondition;

public final class Rejected implements DeliveryState, Outcome {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000025L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:rejected:list");

    private ErrorCondition error;

    public ErrorCondition getError() {
        return error;
    }

    public Rejected setError(ErrorCondition error) {
        this.error = error;
        return this;
    }

    @Override
    public String toString() {
        return "Rejected{" +
               "error=" + error + "}";
    }

    @Override
    public DeliveryStateType getType() {
        return DeliveryStateType.Rejected;
    }
}
