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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Connection options that are applied to the SASL layer.
 */
public class SaslOptions {

	/**
	 * The client default configuration value for SASL enabled state (default is true)
	 */
    public static final boolean DEFAULT_SASL_ENABLED = true;

    private boolean saslEnabled = DEFAULT_SASL_ENABLED;
    private final Set<String> saslAllowedMechs = new LinkedHashSet<>();

    /**
     * Create a new {@link SaslOptions} instance configured with default configuration settings.
     */
    public SaslOptions() {
    }

    @Override
    public SaslOptions clone() {
        return copyInto(new SaslOptions());
    }

    /**
     * @return the saslLayer
     */
    public boolean saslEnabled() {
        return saslEnabled;
    }

    /**
     * @param saslEnabled the saslLayer to set
     *
     * @return this options instance.
     */
    public SaslOptions saslEnabled(boolean saslEnabled) {
        this.saslEnabled = saslEnabled;
        return this;
    }

    /**
     * Adds a mechanism to the list of allowed SASL mechanisms this client will use
     * when selecting from the remote peers offered set of SASL mechanisms.  If no
     * allowed mechanisms are configured then the client will select the first mechanism
     * from the server offered mechanisms that is supported.
     *
     * @param mechanism
     * 		The mechanism to allow.
     *
     * @return this options object for chaining.
     */
    public SaslOptions addAllowedMechanism(String mechanism) {
        Objects.requireNonNull(mechanism, "Cannot add null as an allowed mechanism");
        this.saslAllowedMechs.add(mechanism);
        return this;
    }

    /**
     * @return the current list of allowed SASL Mechanisms.
     */
    public Set<String> allowedMechanisms() {
        return Collections.unmodifiableSet(saslAllowedMechs);
    }

    /**
     * Copy all configuration into the given {@link SaslOptions} from this instance.
     *
     * @param other
     * 		another {@link SaslOptions} instance that will receive the configuration from this instance.
     *
     * @return the options instance that was copied into.
     */
    public SaslOptions copyInto(SaslOptions other) {
        other.saslEnabled(saslEnabled());
        other.saslAllowedMechs.addAll(saslAllowedMechs);

        return other;
    }
}
