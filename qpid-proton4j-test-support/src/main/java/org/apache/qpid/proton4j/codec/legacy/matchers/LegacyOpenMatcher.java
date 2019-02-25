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
package org.apache.qpid.proton4j.codec.legacy.matchers;

import org.apache.qpid.proton4j.amqp.transport.Open;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matches a Proton4J Open to a legacy Version
 */
public class LegacyOpenMatcher extends TypeSafeMatcher<Open> {

    private final org.apache.qpid.proton.amqp.transport.Open legacyOpen;

    /**
     * Create a new OpenMatcher
     */
    public LegacyOpenMatcher(org.apache.qpid.proton.amqp.transport.Open legacyOpen) {
        this.legacyOpen = legacyOpen;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Proton4J Open that matches: ").appendValue(legacyOpen);
    }

    @Override
    protected boolean matchesSafely(Open open) {

        if (open.getContainerId() == null && legacyOpen.getContainerId() != null ||
            open.getContainerId() != null && legacyOpen.getContainerId() == null) {
            return false;
        } else if (open.getContainerId() != null && !open.getContainerId().equals(legacyOpen.getContainerId())) {
            return false;
        }

        // TODO Auto-generated method stub

        return true;
    }
}
