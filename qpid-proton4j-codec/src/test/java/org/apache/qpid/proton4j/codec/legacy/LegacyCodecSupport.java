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

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.Open;

/**
 * Support methods for working with the legacy codec and new proton4j codec
 */
public abstract class LegacyCodecSupport {

    /**
     * Compare a legacy Open to another legacy Open instance.
     *
     * @param legacyOpen1
     *      Array of {@link Open} instances or null
     * @param legacyOpen2
     *      Array of {@link Open} instances or null.
     *
     * @return true if the legacy types are equal.
     */
    public static boolean areEqual(Open legacyOpen1, Open legacyOpen2) {
        if (legacyOpen1 == null && legacyOpen2 == null) {
            return true;
        } else if (legacyOpen1 == null || legacyOpen2 == null) {
            return false;
        }

        if (legacyOpen1.getChannelMax() == null) {
            if (legacyOpen2.getChannelMax() != null) {
                return false;
            }
        } else if (!legacyOpen1.getChannelMax().equals(legacyOpen2.getChannelMax())) {
            return false;
        }

        if (legacyOpen1.getContainerId() == null) {
            if (legacyOpen2.getContainerId() != null) {
                return false;
            }
        } else if (!legacyOpen1.getContainerId().equals(legacyOpen2.getContainerId())) {
            return false;
        }

        if (legacyOpen1.getHostname() == null) {
            if (legacyOpen2.getHostname() != null) {
                return false;
            }
        } else if (!legacyOpen1.getHostname().equals(legacyOpen2.getHostname())) {
            return false;
        }

        if (legacyOpen1.getIdleTimeOut() == null) {
            if (legacyOpen2.getIdleTimeOut() != null) {
                return false;
            }
        } else if (!legacyOpen1.getIdleTimeOut().equals(legacyOpen2.getIdleTimeOut())) {
            return false;
        }

        if (legacyOpen1.getMaxFrameSize() == null) {
            if (legacyOpen2.getMaxFrameSize() != null) {
                return false;
            }
        } else if (!legacyOpen1.getMaxFrameSize().equals(legacyOpen2.getMaxFrameSize())) {
            return false;
        }

        if (legacyOpen1.getProperties() == null) {
            if (legacyOpen2.getProperties() != null) {
                return false;
            }
        } else if (!legacyOpen1.getProperties().equals(legacyOpen2.getProperties())) {
            return false;
        }

        if (!Arrays.equals(legacyOpen1.getDesiredCapabilities(), legacyOpen2.getDesiredCapabilities())) {
            return false;
        }
        if (!Arrays.equals(legacyOpen1.getOfferedCapabilities(), legacyOpen2.getOfferedCapabilities())) {
            return false;
        }
        if (!Arrays.equals(legacyOpen1.getIncomingLocales(), legacyOpen2.getIncomingLocales())) {
            return false;
        }
        if (!Arrays.equals(legacyOpen1.getOutgoingLocales(), legacyOpen2.getOutgoingLocales())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a legacy Open to an Open instance using the proton4j Open type
     *
     * @param newOpen
     *      Array of {@link org.apache.qpid.proton4j.amqp.transport.Open} instances or null
     * @param legacyOpen
     *      Array of {@link Open} instances or null.
     *
     * @return true if the legacy and new types are equal.
     */
    public static boolean areEqual(org.apache.qpid.proton4j.amqp.transport.Open newOpen, Open legacyOpen) {
        return areEqual(legacyOpen, newOpen);
    }

    /**
     * Compare a legacy Open to an Open instance using the proton4j Open type
     *
     * @param legacyOpen
     *      Array of {@link Open} instances or null.
     * @param newOpen
     *      Array of {@link org.apache.qpid.proton4j.amqp.transport.Open} instances or null
     *
     * @return true if the legacy and new types are equal.
     */
    public static boolean areEqual(Open legacyOpen, org.apache.qpid.proton4j.amqp.transport.Open newOpen) {
        if (legacyOpen == null && newOpen == null) {
            return true;
        } else if (legacyOpen == null || newOpen == null) {
            return false;
        }

        if (legacyOpen.getChannelMax() == null) {
            if (newOpen.getChannelMax() != null) {
                return false;
            }
        } else if (legacyOpen.getChannelMax().intValue() != newOpen.getChannelMax().intValue()) {
            return false;
        }

        if (legacyOpen.getContainerId() == null) {
            if (newOpen.getContainerId() != null) {
                return false;
            }
        } else if (!legacyOpen.getContainerId().equals(newOpen.getContainerId())) {
            return false;
        }

        if (legacyOpen.getHostname() == null) {
            if (newOpen.getHostname() != null) {
                return false;
            }
        } else if (!legacyOpen.getHostname().equals(newOpen.getHostname())) {
            return false;
        }

        if (legacyOpen.getIdleTimeOut() == null) {
            if (newOpen.getIdleTimeOut() != null) {
                return false;
            }
        } else if (legacyOpen.getIdleTimeOut().longValue() != newOpen.getIdleTimeOut().longValue()) {
            return false;
        }

        if (legacyOpen.getMaxFrameSize() == null) {
            if (newOpen.getMaxFrameSize() != null) {
                return false;
            }
        } else if (legacyOpen.getMaxFrameSize().longValue() != newOpen.getMaxFrameSize().longValue()) {
            return false;
        }

        if (legacyOpen.getProperties() == null) {
            if (newOpen.getProperties() != null) {
                return false;
            }
        } else if (!legacyOpen.getProperties().equals(newOpen.getProperties())) {
            return false;
        }

        if (!LegacyCodecSupport.areEqual(legacyOpen.getDesiredCapabilities(), newOpen.getDesiredCapabilities())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyOpen.getOfferedCapabilities(), newOpen.getOfferedCapabilities())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyOpen.getIncomingLocales(), newOpen.getIncomingLocales())) {
            return false;
        }
        if (!LegacyCodecSupport.areEqual(legacyOpen.getOutgoingLocales(), newOpen.getOutgoingLocales())) {
            return false;
        }

        return true;
    }

    /**
     * Compare a legacy Symbol Array to an Symbol array using the proton4j Symbol type
     *
     * @param newSymbols
     *      Array of {@link org.apache.qpid.proton4j.amqp.Symbol} instances or null
     * @param legacySymbols
     *      Array of {@link Symbol} instances or null.
     *
     * @return true if the arrays contain matching underlying Symbol values in the same order.
     */
    public static boolean areEqual(org.apache.qpid.proton4j.amqp.Symbol[] newSymbols, Symbol[] legacySymbols) {
        return areEqual(legacySymbols, newSymbols);
    }

    /**
     * Compare a legacy Symbol Array to an Symbol array using the proton4j Symbol type
     *
     * @param legacySymbols
     *      Array of {@link Symbol} instances or null.
     * @param newSymbols
     *      Array of {@link org.apache.qpid.proton4j.amqp.Symbol} instances or null
     *
     * @return true if the arrays contain matching underlying Symbol values in the same order.
     */
    public static boolean areEqual(Symbol[] legacySymbols, org.apache.qpid.proton4j.amqp.Symbol[] newSymbols) {
        if (legacySymbols == null && newSymbols == null) {
            return true;
        } else if (legacySymbols == null || newSymbols == null) {
            return false;
        }

        int length = legacySymbols.length;
        if (newSymbols.length != length) {
            return false;
        }

        for (int i=0; i<length; i++) {
            Object symbolOld = legacySymbols[i];
            Object symbolNew = newSymbols[i];
            if (!(symbolOld == null ? symbolNew == null : symbolOld.toString().equals(symbolNew.toString()))) {
                return false;
            }
        }

        return true;
    }
}
