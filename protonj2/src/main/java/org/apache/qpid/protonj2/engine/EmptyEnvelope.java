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
package org.apache.qpid.protonj2.engine;

import org.apache.qpid.protonj2.types.transport.Performative.PerformativeHandler;

/**
 * An empty envelope which can be used to drive transport activity when idle.
 */
public final class EmptyEnvelope extends IncomingAMQPEnvelope {

	/**
	 * The singleton instance of the {@link EmptyEnvelope} type.
	 */
    public static final EmptyEnvelope INSTANCE = new EmptyEnvelope();

    private EmptyEnvelope() {
        super();
    }

    @Override
    public String toString() {
        return "Empty Frame";
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, E context) {
        // Nothing to do for empty frame.
    }
}
