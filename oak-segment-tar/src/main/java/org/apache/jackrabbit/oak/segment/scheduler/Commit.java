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

package org.apache.jackrabbit.oak.segment.scheduler;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A {@code Commit} instance represents a set of related changes, which when applied to
 * a base node state result in a new node state.
 */
public class Commit {
    private final SegmentNodeBuilder changes;
    private final CommitHook hook;
    private final CommitInfo info;
    
    public Commit(@Nonnull NodeBuilder changes,
            @Nonnull CommitHook hook, @Nonnull CommitInfo info) {
        checkNotNull(changes);
        checkArgument(changes instanceof SegmentNodeBuilder);
        this.changes = (SegmentNodeBuilder) changes;

        this.hook = checkNotNull(hook);
        this.info = checkNotNull(info);
    }

    /**
     * Apply the changes represented by this commit to the passed {@code base}
     * node state.
     *
     * @param base   the base node state to apply this commit to
     * @return       the resulting state from applying this commit to {@code base}.
     * @throws CommitFailedException  if the commit cannot be applied to {@code base}.
     *                                (e.g. because of a conflict.)
     */
    public NodeState apply(NodeState base) throws CommitFailedException {
        return null;
    }

    public SegmentNodeBuilder getChanges() {
        return changes;
    }

    public CommitHook getHook() {
        return hook;
    }

    public CommitInfo getInfo() {
        return info;
    }
    
    
}