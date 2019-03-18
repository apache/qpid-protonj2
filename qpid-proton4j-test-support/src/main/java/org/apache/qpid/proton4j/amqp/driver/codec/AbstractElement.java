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

abstract class AbstractElement<T> implements Element<T> {

    private Element<?> parent;
    private Element<?> next;
    private Element<?> prev;

    AbstractElement(Element<?> parent, Element<?> prev) {
        this.parent = parent;
        this.prev = prev;
    }

    protected boolean isElementOfArray() {
        return parent instanceof ArrayElement && !(((ArrayElement) parent()).isDescribed() && this == parent.child());
    }

    @Override
    public Element<?> next() {
        // TODO
        return next;
    }

    @Override
    public Element<?> prev() {
        // TODO
        return prev;
    }

    @Override
    public Element<?> parent() {
        // TODO
        return parent;
    }

    @Override
    public void setNext(Element<?> elt) {
        next = elt;
    }

    @Override
    public void setPrev(Element<?> elt) {
        prev = elt;
    }

    @Override
    public void setParent(Element<?> elt) {
        parent = elt;
    }

    @Override
    public Element<?> replaceWith(Element<?> elt) {
        if (parent != null) {
            elt = parent.checkChild(elt);
        }

        elt.setPrev(prev);
        elt.setNext(next);
        elt.setParent(parent);

        if (prev != null) {
            prev.setNext(elt);
        }
        if (next != null) {
            next.setPrev(elt);
        }

        if (parent != null && parent.child() == this) {
            parent.setChild(elt);
        }

        return elt;
    }

    @Override
    public String toString() {
        return String.format("%s[%h]{parent=%h, prev=%h, next=%h}", this.getClass().getSimpleName(), System.identityHashCode(this),
            System.identityHashCode(parent), System.identityHashCode(prev), System.identityHashCode(next));
    }

    abstract String startSymbol();

    abstract String stopSymbol();

    @Override
    public void render(StringBuilder sb) {
        if (canEnter()) {
            sb.append(startSymbol());
            Element<?> el = child();
            boolean first = true;
            while (el != null) {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }
                el.render(sb);
                el = el.next();
            }
            sb.append(stopSymbol());
        } else {
            sb.append(getDataType()).append(" ").append(getValue());
        }
    }
}
