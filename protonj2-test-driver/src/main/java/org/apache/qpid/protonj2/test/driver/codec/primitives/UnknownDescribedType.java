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
package org.apache.qpid.protonj2.test.driver.codec.primitives;

public class UnknownDescribedType implements DescribedType {

    private final Object descriptor;
    private final Object described;

    public UnknownDescribedType(final Object descriptor, final Object described) {
        this.descriptor = descriptor;
        this.described = described;
    }

    @Override
    public Object getDescriptor() {
        return descriptor;
    }

    @Override
    public Object getDescribed() {
        return described;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !DescribedType.class.isAssignableFrom(o.getClass())) {
            return false;
        }

        final DescribedType that = (DescribedType) o;

        if (described != null ? !described.equals(that.getDescribed()) : that.getDescriptor() != null) {
            return false;
        }
        if (descriptor != null ? !descriptor.equals(that.getDescriptor()) : that.getDescriptor() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = descriptor != null ? descriptor.hashCode() : 0;
        result = 31 * result + (described != null ? described.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "UnknownDescribedType{" + "descriptor=" + descriptor + ", described=" + described + '}';
    }
}
