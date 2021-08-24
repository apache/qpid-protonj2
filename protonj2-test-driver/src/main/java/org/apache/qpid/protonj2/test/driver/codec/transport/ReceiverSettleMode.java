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
package org.apache.qpid.protonj2.test.driver.codec.transport;

import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedByte;

public enum ReceiverSettleMode {

    FIRST(0), SECOND(1);

    private final UnsignedByte value;

    private ReceiverSettleMode(int value) {
        this.value = UnsignedByte.valueOf((byte)value);
    }

    public static ReceiverSettleMode valueOf(UnsignedByte value) {
        return value == null ? FIRST : ReceiverSettleMode.valueOf(value.byteValue());
    }

    public static ReceiverSettleMode valueOf(byte value) {
        switch (value) {
            case 0:
                return ReceiverSettleMode.FIRST;
            case 1:
                return ReceiverSettleMode.SECOND;
            default:
                throw new IllegalArgumentException("The value can be only 0 (for FIRST) and 1 (for SECOND)");
        }
    }

    public byte byteValue() {
        return value.byteValue();
    }

    public UnsignedByte getValue() {
        return value;
    }
}
