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
package org.messaginghub.amqperative.impl.sasl;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.qpid.proton4j.amqp.Symbol;

/**
 * Used to select a matching mechanism from the server offered list of mechanisms
 */
public final class SaslMechanismSelector {

    // TODO - internally we should use Symbol ?

    private final Set<String> allowedMechanisms;
    private final Set<String> blackListedMechanisms;

    public SaslMechanismSelector(Set<String> allowed, Set<String> blacklisted) {
        this.allowedMechanisms = allowed;
        this.blackListedMechanisms = blacklisted;
    }

    public Mechanism select(Symbol[] serverMechs, SaslCredentialsProvider credentials) {
        Mechanism selected = null;

        Set<String> matching = new LinkedHashSet<>(serverMechs.length);
        for (Symbol serverMech : serverMechs) {
            matching.add(serverMech.toString());
        }

        if (blackListedMechanisms != null) {
            matching.removeAll(blackListedMechanisms);
        }

        matching.retainAll(allowedMechanisms);

        for (String match : matching) {
            SaslMechanisms option = SaslMechanisms.valueOf(match);
            if (option.isApplicable(credentials)) {
                selected = option.createMechanism();
            }
        }

        return selected;
    }
}
