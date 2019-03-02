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
package org.apache.qpid.proton4j.codec.legacy;

import org.apache.qpid.proton.amqp.transport.Attach;
import org.apache.qpid.proton.amqp.transport.Begin;

/**
 * Adapt from the new proton4j Attach to the legacy proton-j Attach
 */
public class LegacyAttachTypeAdapter extends LegacyTypeAdapter<Attach, org.apache.qpid.proton4j.amqp.transport.Attach> {

    public LegacyAttachTypeAdapter(Attach legacyType) {
        super(legacyType);
    }

    @Override
    public boolean equals(Object value) {
        if (this == value) {
            return true;
        } else if (value == null) {
            return false;
        }

        if (value instanceof org.apache.qpid.proton4j.amqp.transport.Attach) {
            return LegacyCodecSupport.areEqual(legacyType, (org.apache.qpid.proton4j.amqp.transport.Attach) value);
        } else if (value instanceof Begin) {
            return LegacyCodecSupport.areEqual(legacyType, (Attach) value);
        }

        return false;
    }
}
