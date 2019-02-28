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

import java.util.Arrays;

import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton4j.codec.legacy.matchers.LegacyOpenMatcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Adapt from the new proton4j Open to the legacy proton-j Open
 */
public class LegacyOpenTypeAdapter extends LegacyTypeAdapter<Open, org.apache.qpid.proton4j.amqp.transport.Open> {

    public LegacyOpenTypeAdapter(Open legacyType) {
        super(legacyType);
    }

    @Override
    public TypeSafeMatcher<org.apache.qpid.proton4j.amqp.transport.Open> createMatcher() {
        return new LegacyOpenMatcher(legacyType);
    }

    @Override
    public boolean equals(Object value) {
        if (this == value) {
            return true;
        } else if (value == null) {
            return false;
        }

        if (value instanceof org.apache.qpid.proton4j.amqp.transport.Open) {
            return equals((org.apache.qpid.proton4j.amqp.transport.Open) value);
        } else if (value instanceof Open) {
            return equals((Open) value);
        }

        return false;
    }

    public boolean equals(Open open) {
        if (legacyType.getChannelMax() == null) {
            if (open.getChannelMax() != null) {
                return false;
            }
        } else if (!legacyType.getChannelMax().equals(open.getChannelMax())) {
            return false;
        }

        if (legacyType.getContainerId() == null) {
            if (open.getContainerId() != null) {
                return false;
            }
        } else if (!legacyType.getContainerId().equals(open.getContainerId())) {
            return false;
        }

        if (legacyType.getHostname() == null) {
            if (open.getHostname() != null) {
                return false;
            }
        } else if (!legacyType.getHostname().equals(open.getHostname())) {
            return false;
        }

        if (legacyType.getIdleTimeOut() == null) {
            if (open.getIdleTimeOut() != null) {
                return false;
            }
        } else if (!legacyType.getIdleTimeOut().equals(open.getIdleTimeOut())) {
            return false;
        }

        if (legacyType.getMaxFrameSize() == null) {
            if (open.getMaxFrameSize() != null) {
                return false;
            }
        } else if (!legacyType.getMaxFrameSize().equals(open.getMaxFrameSize())) {
            return false;
        }

        if (legacyType.getProperties() == null) {
            if (open.getProperties() != null) {
                return false;
            }
        } else if (!legacyType.getProperties().equals(open.getProperties())) {
            return false;
        }

        if (!Arrays.equals(legacyType.getDesiredCapabilities(), open.getDesiredCapabilities())) {
            return false;
        }
        if (!Arrays.equals(legacyType.getOfferedCapabilities(), open.getOfferedCapabilities())) {
            return false;
        }
        if (!Arrays.equals(legacyType.getIncomingLocales(), open.getIncomingLocales())) {
            return false;
        }
        if (!Arrays.equals(legacyType.getOutgoingLocales(), open.getOutgoingLocales())) {
            return false;
        }

        return true;
    }

    public boolean equals(org.apache.qpid.proton4j.amqp.transport.Open open) {
        return false;
    }
}
