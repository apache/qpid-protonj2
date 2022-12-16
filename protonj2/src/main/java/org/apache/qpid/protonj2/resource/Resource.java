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

package org.apache.qpid.protonj2.resource;

/**
 * Base resource type that provides API for managing ownership and cleaning
 * up resources that require explicit life-cycle management.
 *
 * @param <T> The type that is held in this resource.
 */
public interface Resource<T extends Resource<T>> extends AutoCloseable {

    /**
     * Close the resource and free any managed resources it might own. Calling close
     * on a closed resource should not result in any exceptions being thrown.
     */
    @Override
    void close();

    /**
     * Transfers ownership of this resource from one owner to another.
     * <p>
     * Upon transferring a resource the original referenced value is closed and its
     * contents are migrated to a new view that has the same state as the original
     * view but it now the property of the caller and who's life-cycle now must be
     * managed by the caller.
     *
     * @return the a new transfered view of the given resource now owned by the caller.
     */
    T transfer();

    /**
     * A {@link Resource} is closed either following a call to the {@link #close()} method
     * or by the resource being transfered by way of the {@link #transfer()} method.
     *
     * @return true if this resource is closed, either by being transfered or by direct {@link #close()}.
     */
    boolean isClosed();

}
