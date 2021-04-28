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
package org.apache.qpid.protonj2.client.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.qpid.protonj2.client.ReconnectLocation;

/**
 * Manages the list of available reconnect entries that are used to connect
 * and recover a connection.
 */
public class ReconnectLocationPool {

    private final LinkedList<ReconnectLocation> entries;

    /**
     * Creates an empty {@link ReconnectLocationPool}.
     */
    public ReconnectLocationPool() {
        this.entries = new LinkedList<ReconnectLocation>();
    }

    /**
     * Creates a new {@link ReconnectLocationPool} with the provided {@link ReconnectLocation} values.
     *
     * @param backups
     * 		a list of location where a reconnection attempt should be made.
     */
    public ReconnectLocationPool(List<ReconnectLocation> backups) {
        this.entries = new LinkedList<ReconnectLocation>();

        if (backups != null) {
            for (ReconnectLocation entry : backups) {
                this.add(entry);
            }
        }
    }

    /**
     * @return the current size of the entry pool.
     */
    public int size() {
        synchronized (entries) {
            return entries.size();
        }
    }

    /**
     * @return true if the entry pool is empty.
     */
    public boolean isEmpty() {
        synchronized (entries) {
            return entries.isEmpty();
        }
    }

    /**
     * Returns the next entry in the pool of entries.  The entry will be shifted to the
     * end of the list and not be attempted again until the full list has been
     * returned once.
     *
     * @return the next entry that should be used for a connection attempt.
     */
    public ReconnectLocation getNext() {
    	ReconnectLocation next = null;
        synchronized (entries) {
            if (!entries.isEmpty()) {
                next = entries.removeFirst();
                entries.addLast(next);
            }
        }

        return next;
    }

    /**
     * Randomizes the order of the list of entries contained within the pool.
     */
    public void shuffle() {
        synchronized (entries) {
            Collections.shuffle(entries);
        }
    }

    /**
     * Adds a new entry to the pool if not already contained within.
     *
     * @param entry
     *        The new {@link ReconnectLocation} to add to the pool.
     */
    public void add(ReconnectLocation entry) {
        if (entry != null) {
            synchronized (entries) {
                if (!contains(entry)) {
                    entries.add(entry);
                }
            }
        }
    }

    /**
     * Adds a list of new {@link ReconnectLocation} values to the pool if not already contained within.
     *
     * @param additions
     *        The new list of {@link ReconnectLocation} to add to the pool.
     */
    public void addAll(List<ReconnectLocation> additions) {
        if (additions != null && !additions.isEmpty()) {
            synchronized (entries) {
                for (ReconnectLocation entry : additions) {
                    add(entry);
                }
            }
        }
    }

    /**
     * Adds a new {@link ReconnectLocation} to the pool if not already contained within.
     *
     * The {@link ReconnectLocation} is added to the head of the pooled {@link ReconnectLocation} list and will be the
     * next value that is returned from the pool.
     *
     * @param entry
     *        The new {@link ReconnectLocation} to add to the pool.
     */
    public void addFirst(ReconnectLocation entry) {
        if (entry != null) {
            synchronized (entries) {
                if (!contains(entry)) {
                    entries.addFirst(entry);
                }
            }
        }
    }

    /**
     * Remove a {@link ReconnectLocation} from the pool if present, otherwise has no effect.
     *
     * @param entry
     *        The {@link ReconnectLocation} to attempt to remove from the pool.
     *
     * @return true if the given {@link ReconnectLocation} was removed from the pool.
     */
    public boolean remove(ReconnectLocation entry) {
        if (entry != null) {
	        synchronized (entries) {
	            for (ReconnectLocation candidate : entries) {
	                if (compareEntries(entry, candidate)) {
	                    return entries.remove(candidate);
	                }
	            }
	        }
        }

        return false;
    }

    /**
     * Removes all currently configured {@link ReconnectLocation} from the pool, no new {@link ReconnectLocation} values
     * will be served from this pool until new ones are added.
     */
    public void removeAll() {
        synchronized (entries) {
            entries.clear();
        }
    }

    /**
     * Removes all currently configured {@link ReconnectLocation} values from the pool and replaces them with
     * the new set given.
     *
     * @param replacements
     * 		The new set of reconnect {@link ReconnectLocation} values to serve from this pool.
     */
    public void replaceAll(List<ReconnectLocation> replacements) {
        synchronized (entries) {
            entries.clear();
            addAll(replacements);
        }
    }

    /**
     * Gets the current list of {@link ReconnectLocation} values. The returned list is a copy.
     *
     * @return a copy of the current list of {@link ReconnectLocation} values in the pool.
     */
    public List<ReconnectLocation> getList() {
        synchronized (entries) {
            return new ArrayList<>(entries);
        }
    }

    @Override
    public String toString() {
        synchronized (entries) {
            return "ReconnectLocationPool { " + entries + " }";
        }
    }

    //----- Internal methods that require the locks be held ------------------//

    private boolean contains(ReconnectLocation newEntry) {
        boolean result = false;
        for (ReconnectLocation entry : entries) {
            if (compareEntries(newEntry, entry)) {
                result = true;
                break;
            }
        }

        return result;
    }

    private boolean compareEntries(final ReconnectLocation first, final ReconnectLocation second) {
        boolean result = false;

        if (first == null || second == null) {
            return result;
        } else if (first.getPort() == second.getPort()) {
            final String firstHost = first.getHost();
            final String secondHost = second.getHost();

            if (firstHost.equalsIgnoreCase(secondHost)) {
                result = true;
            }
        }

        return result;
    }
}
