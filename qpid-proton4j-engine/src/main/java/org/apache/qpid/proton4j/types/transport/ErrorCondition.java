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
package org.apache.qpid.proton4j.types.transport;

import java.util.Collections;
import java.util.Map;

import org.apache.qpid.proton4j.types.Symbol;
import org.apache.qpid.proton4j.types.UnsignedLong;

public final class ErrorCondition {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x000000000000001dL);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:error:list");

    private final Symbol condition;
    private final String description;
    private final Map<Symbol, Object> info;

    public ErrorCondition(Symbol condition, String description) {
        this(condition, description, null);
    }

    public ErrorCondition(Symbol condition, String description, Map<Symbol, Object> info) {
        this.condition = condition;
        this.description = description;
        this.info = info != null ? Collections.unmodifiableMap(info) : null;
    }

    public Symbol getCondition() {
        return condition;
    }

    public String getDescription() {
        return description;
    }

    public Map<Symbol, Object> getInfo() {
        return info;
    }

    public ErrorCondition copy() {
        return new ErrorCondition(condition, description, info);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ErrorCondition that = (ErrorCondition) o;

        if (condition != null ? !condition.equals(that.condition) : that.condition != null) {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null) {
            return false;
        }
        if (info != null ? !info.equals(that.info) : that.info != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = condition != null ? condition.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (info != null ? info.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Error{" +
               "condition=" + condition +
               ", description='" + description + '\'' +
               ", info=" + info +
               '}';
    }
}
