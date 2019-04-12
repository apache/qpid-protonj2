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
package org.apache.qpid.proton4j.amqp.driver.codec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.qpid.proton4j.amqp.DescribedType;

public abstract class ListDescribedType implements DescribedType {

    private final ArrayList<Object> fields;

    public ListDescribedType(int numberOfFields) {
        fields = new ArrayList<>(numberOfFields);

        for (int i = 0; i < numberOfFields; ++i) {
            fields.add(null);
        }
    }

    public ListDescribedType(int numberOfFields, List<Object> described) {
        if (described.size() > numberOfFields) {
            throw new IllegalArgumentException("List encoded exceeds expected number of elements for this type");
        }

        fields = new ArrayList<>(numberOfFields);

        for (int i = 0; i < numberOfFields; ++i) {
            if (i < described.size()) {
                fields.add(described.get(i));
            } else {
                fields.add(null);
            }
        }
    }

    @Override
    public List<Object> getDescribed() {
        // Return a List containing only the 'used fields' (i.e up to the
        // highest field used)
        int highestSetFeild = 0;
        for (int i = 0; i < fields.size(); ++i) {
            if (fields.get(i) != null) {
                highestSetFeild = i;
            }
        }

        // Create a list with the fields in the correct positions.
        List<Object> list = new ArrayList<>();
        for (int j = 0; j <= highestSetFeild; j++) {
            list.add(fields.get(j));
        }

        return list;
    }

    public Object getFieldValue(int index) {
        if (index < fields.size()) {
            return fields.get(index);
        } else {
            throw new AssertionError("Request for unknown field in type: " + this);
        }
    }

    protected int getHighestSetFieldId() {
        int numUsedFields = 0;
        for (Object element : fields) {
            if (element != null) {
                numUsedFields++;
            }
        }

        return numUsedFields;
    }

    protected ArrayList<Object> getList() {
        return fields;
    }

    protected Object[] getFields() {
        return fields.toArray();
    }

    @Override
    public String toString() {
        return "ListDescribedType [descriptor=" + getDescriptor() + " fields=" + Arrays.toString(getFields()) + "]";
    }
}