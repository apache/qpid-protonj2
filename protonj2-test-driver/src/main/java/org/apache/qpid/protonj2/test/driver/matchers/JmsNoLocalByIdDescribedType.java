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

package org.apache.qpid.protonj2.test.driver.matchers;

import org.apache.qpid.protonj2.test.driver.codec.primitives.Symbol;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnknownDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.primitives.UnsignedLong;

/**
 * JMS No Local described type that uses an unsigned long ID for the descriptor
 */
public class JmsNoLocalByIdDescribedType extends UnknownDescribedType {

    /**
     * Key name used when add the selector type to the filters map.
     */
    public static final String JMS_NO_LOCAL_KEY = "no-local";

    /**
     * Symbolic key name used when add the selector type to the filters map.
     */
    public static final Symbol JMS_NO_LOCAL_SYMBOL_KEY = Symbol.valueOf(JMS_NO_LOCAL_KEY);

    public static final UnsignedLong JMS_NO_LOCAL_ULONG_DESCRIPTOR = UnsignedLong.valueOf(0x0000468C00000003L);

    public JmsNoLocalByIdDescribedType() {
        super(JMS_NO_LOCAL_ULONG_DESCRIPTOR, "NoLocalFilter{}");
    }

    @Override
    public boolean equals(final Object o) {
        return super.equals(o);
    }

    @Override
    public String toString() {
        return "JmsNoLocalByIdDescribedType{ " + getDescribed() + " }";
    }
}
