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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.Type.LONG;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.scheduler.Scheduler;
import org.apache.jackrabbit.oak.segment.scheduler.SchedulerOptions;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockBasedScheduler implements Scheduler<SchedulerOptions> {
    
    public static class LockBasedSchedulerBuilder {
        @Nonnull
        private final SegmentReader reader;

        @Nonnull
        private final Revisions revisions;

        @Nonnull
        private StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        
        private boolean dispatchChanges = true;

        private long maximumBackoff = MILLISECONDS.convert(10, SECONDS);
        
        private LockBasedSchedulerBuilder(@Nonnull Revisions revisions,
                @Nonnull SegmentReader reader) {
            this.revisions = revisions;
            this.reader = reader;
        }
        
        /**
         * {@link StatisticsProvider} for collecting statistics related to SegmentStore
         * @param statisticsProvider
         * @return this instance
         */
        @Nonnull
        public LockBasedSchedulerBuilder withStatisticsProvider(@Nonnull StatisticsProvider statisticsProvider) {
            this.statsProvider = checkNotNull(statisticsProvider);
            return this;
        }
        
        @Nonnull
        public LockBasedSchedulerBuilder dispatchChanges(boolean dispatchChanges) {
            this.dispatchChanges = dispatchChanges;
            return this;
        }
        
        @Nonnull
        public LockBasedSchedulerBuilder withMaximumBackoff(long maximumBackoff) {
            this.maximumBackoff = maximumBackoff;
            return this;
        }
        
        @Nonnull
        public LockBasedScheduler build() {
            return new LockBasedScheduler(this);
        }
        
    }
    
    public static LockBasedSchedulerBuilder builder(@Nonnull Revisions revisions, @Nonnull SegmentReader reader) {
        return new LockBasedSchedulerBuilder(checkNotNull(revisions), checkNotNull(reader));
    }

    
    private static final Logger log = LoggerFactory.getLogger(LockBasedScheduler.class);

    /**
     * Flag controlling the commit lock fairness
     */
    private static final boolean COMMIT_FAIR_LOCK = Boolean
            .parseBoolean(System.getProperty("oak.segmentNodeStore.commitFairLock", "true"));

    /**
     * Sets the number of seconds to wait for the attempt to grab the lock to
     * create a checkpoint
     */
    private int checkpointsLockWaitTime = Integer.getInteger("oak.checkpoints.lockWaitTime", 10);

    static final String ROOT = "root";

    /**
     * Semaphore that controls access to the {@link #head} variable. Only a
     * single local commit is allowed at a time. When such a commit is in
     * progress, no external updates will be seen.
     */
    private final Semaphore commitSemaphore = new Semaphore(1, COMMIT_FAIR_LOCK);

    @Nonnull
    private final SegmentReader reader;

    @Nonnull
    private final Revisions revisions;

    private final AtomicReference<SegmentNodeState> head;

    private final ChangeDispatcher changeDispatcher;
    
    private final Random random = new Random();
    
    private final SegmentNodeStoreStats stats;
    
    private final long maximumBackoff;

    public LockBasedScheduler(LockBasedSchedulerBuilder builder) {
        if (COMMIT_FAIR_LOCK) {
            log.info("Initializing SegmentNodeStore with the commitFairLock option enabled.");
        }

        this.reader = builder.reader;
        this.revisions = builder.revisions;

        if (builder.dispatchChanges) {
            this.changeDispatcher = new ChangeDispatcher(getRoot());
        } else {
            this.changeDispatcher = null;
        }
        
        this.stats = new SegmentNodeStoreStats(builder.statsProvider);
        this.maximumBackoff = builder.maximumBackoff;
        this.head = new AtomicReference<SegmentNodeState>(reader.readHeadState(revisions));
    }

    @Override
    public NodeState getHeadNodeState() {
        if (commitSemaphore.tryAcquire()) {
            try {
                refreshHead(true);
            } finally {
                commitSemaphore.release();
            }
        }
        return head.get();
    }
    
    private NodeState getRoot() {
        return getHeadNodeState().getChildNode(ROOT);
    }

    @Override
    public NodeState schedule(NodeBuilder changes, CommitHook commitHook, CommitInfo info,
            SchedulerOptions schedulingOptions) throws CommitFailedException {
        checkArgument(changes instanceof SegmentNodeBuilder);
        SegmentNodeBuilder snb = (SegmentNodeBuilder) changes;
        checkArgument(snb.isRootBuilder());
        checkNotNull(commitHook);

        boolean queued = false;

        try {
            long queuedTime = -1;

            if (commitSemaphore.availablePermits() < 1) {
                queuedTime = System.nanoTime();
                stats.onCommitQueued();
                queued = true;
            }

            commitSemaphore.acquire();
            try {
                if (queued) {
                    long dequeuedTime = System.nanoTime();
                    stats.dequeuedAfter(dequeuedTime - queuedTime);
                    stats.onCommitDequeued();
                }

                long beforeCommitTime = System.nanoTime();

                NodeState merged = execute(snb, commitHook, info);
                snb.reset(merged);

                long afterCommitTime = System.nanoTime();
                stats.committedAfter(afterCommitTime - beforeCommitTime);
                stats.onCommit();

                return merged;
            } finally {
                commitSemaphore.release();
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new CommitFailedException("Segment", 2, "Merge interrupted", e);
        } catch (SegmentOverflowException e) {
            throw new CommitFailedException("Segment", 3, "Merge failed", e);
        }
    }

    private NodeState execute(SegmentNodeBuilder changes, CommitHook hook, CommitInfo info)
            throws CommitFailedException, InterruptedException {
        // only do the merge if there are some changes to commit
        if (!SegmentNodeState.fastEquals(changes.getBaseState(), changes.getNodeState())) {
            long timeout = optimisticMerge(changes, hook, info);
            if (timeout >= 0) {
                pessimisticMerge(changes, hook, info, timeout);
            }
        }
        return head.get().getChildNode(ROOT);
    }
    
    private long optimisticMerge(SegmentNodeBuilder changes, CommitHook hook, CommitInfo info)
            throws CommitFailedException, InterruptedException {
        long timeout = 1;

        // use exponential backoff in case of concurrent commits
        for (long backoff = 1; backoff < maximumBackoff; backoff *= 2) {
            long start = System.nanoTime();

            refreshHead(true);
            SegmentNodeState state = head.get();
            if (state.hasProperty("token")
                    && state.getLong("timeout") >= currentTimeMillis()) {
                // someone else has a pessimistic lock on the journal,
                // so we should not try to commit anything yet
            } else {
                SegmentNodeBuilder builder = prepare(state, changes, hook, info);
                // use optimistic locking to update the journal
                if (setHead(state, builder.getNodeState(), info)) {
                    return -1;
                }
            }

            // someone else was faster, so wait a while and retry later
            Thread.sleep(backoff, random.nextInt(1000000));

            long stop = System.nanoTime();
            if (stop - start > timeout) {
                timeout = stop - start;
            }
        }

        return MILLISECONDS.convert(timeout, NANOSECONDS);
    }

    private void pessimisticMerge(SegmentNodeBuilder changes, CommitHook hook, CommitInfo info, long timeout)
            throws CommitFailedException, InterruptedException {
        while (true) {
            long now = currentTimeMillis();
            SegmentNodeState state = head.get();
            if (state.hasProperty("token")
                    && state.getLong("timeout") >= now) {
                // locked by someone else, wait until unlocked or expired
                Thread.sleep(
                        Math.min(state.getLong("timeout") - now, 1000),
                        random.nextInt(1000000));
            } else {
                // attempt to acquire the lock
                SegmentNodeBuilder builder = state.builder();
                builder.setProperty("token", UUID.randomUUID().toString());
                builder.setProperty("timeout", now + timeout);

                if (setHead(state, builder.getNodeState(), info)) {
                     // lock acquired; rebase, apply commit hooks, and unlock
                    builder = prepare(state, changes, hook, info);
                    builder.removeProperty("token");
                    builder.removeProperty("timeout");

                    // complete the commit
                    if (setHead(state, builder.getNodeState(), info)) {
                        return;
                    }
                }
            }
        }
    }
    
    private SegmentNodeBuilder prepare(SegmentNodeState state, SegmentNodeBuilder changes, CommitHook hook, CommitInfo info) throws CommitFailedException {
        SegmentNodeBuilder builder = state.builder();
        if (SegmentNodeState.fastEquals(changes.getBaseState(), state.getChildNode(ROOT))) {
            // use a shortcut when there are no external changes
            NodeState before = changes.getBaseState();
            NodeState after = changes.getNodeState();
            
            builder.setChildNode(
                    ROOT, hook.processCommit(before, after, info));
        } else {
            // there were some external changes, so do the full rebase
            ConflictAnnotatingRebaseDiff diff =
                    new ConflictAnnotatingRebaseDiff(builder.child(ROOT));
            changes.getNodeState().compareAgainstBaseState(changes.getBaseState(), diff);
            // apply commit hooks on the rebased changes         
            builder.setChildNode(ROOT, hook.processCommit(
                    builder.getBaseState().getChildNode(ROOT),
                    builder.getNodeState().getChildNode(ROOT),
                    info));
        }
        return builder;
    }
    
    private boolean setHead(SegmentNodeState before, SegmentNodeState after, CommitInfo info) {
        refreshHead(true);
        if (revisions.setHead(before.getRecordId(), after.getRecordId())) {
            head.set(after);
            contentChanged(after.getChildNode(ROOT), info);
            refreshHead(true);
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        checkArgument(lifetime > 0);
        checkNotNull(properties);
        String name = UUID.randomUUID().toString();
        try {
            CPCreator cpc = new CPCreator(name, lifetime, properties);
            if (locked(cpc, checkpointsLockWaitTime, TimeUnit.SECONDS)) {
                return name;
            }
            log.warn("Failed to create checkpoint {} in {} seconds.", name, checkpointsLockWaitTime);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            log.error("Failed to create checkpoint {}.", name, e);
        } catch (Exception e) {
            log.error("Failed to create checkpoint {}.", name, e);
        }
        return name;
    }

    /**
     * Execute the passed callable with trying to acquire this store's commit
     * lock.
     * 
     * @param timeout
     *            the maximum time to wait for the store's commit lock
     * @param unit
     *            the time unit of the {@code timeout} argument
     * @param c
     *            callable to execute
     * @return {@code false} if the store's commit lock cannot be acquired, the
     *         result of {@code c.call()} otherwise.
     * @throws Exception
     */
    boolean locked(Callable<Boolean> c, long timeout, TimeUnit unit) throws Exception {
        if (commitSemaphore.tryAcquire(timeout, unit)) {
            try {
                return c.call();
            } finally {
                // Explicitly give up reference to the previous root state
                // otherwise they would block cleanup. See OAK-3347
                refreshHead(true);
                commitSemaphore.release();
            }
        }
        return false;
    }

    @Override
    public boolean removeCheckpoint(String name) {
        checkNotNull(name);

        // try 5 times
        for (int i = 0; i < 5; i++) {
            if (commitSemaphore.tryAcquire()) {
                try {
                    refreshHead(true);

                    SegmentNodeState state = head.get();
                    SegmentNodeBuilder builder = state.builder();

                    NodeBuilder cp = builder.child("checkpoints").child(
                            name);
                    if (cp.exists()) {
                        cp.remove();
                        SegmentNodeState newState = builder.getNodeState();
                        if (revisions.setHead(state.getRecordId(), newState.getRecordId())) {
                            refreshHead(false);
                            return true;
                        }
                    }
                } finally {
                    commitSemaphore.release();
                }
            }
        }
        return false;
    }

    /**
     * Refreshes the head state. Should only be called while holding a permit
     * from the {@link #commitSemaphore}.
     * 
     * @param dispatchChanges
     *            if set to true the changes would also be dispatched
     */
    private void refreshHead(boolean dispatchChanges) {
        SegmentNodeState state = reader.readHeadState(revisions);
        if (!state.getRecordId().equals(head.get().getRecordId())) {
            head.set(state);
            if (dispatchChanges) {
                contentChanged(state.getChildNode(ROOT), CommitInfo.EMPTY_EXTERNAL);
            }
        }
    }

    private void contentChanged(NodeState root, CommitInfo info) {
        if (changeDispatcher != null) {
            changeDispatcher.contentChanged(root, info);
        }
    }

    private final class CPCreator implements Callable<Boolean> {

        private final String name;
        private final long lifetime;
        private final Map<String, String> properties;

        CPCreator(String name, long lifetime, Map<String, String> properties) {
            this.name = name;
            this.lifetime = lifetime;
            this.properties = properties;
        }

        @Override
        public Boolean call() {
            long now = System.currentTimeMillis();

            refreshHead(true);

            SegmentNodeState state = head.get();
            SegmentNodeBuilder builder = state.builder();

            NodeBuilder checkpoints = builder.child("checkpoints");
            for (String n : checkpoints.getChildNodeNames()) {
                NodeBuilder cp = checkpoints.getChildNode(n);
                PropertyState ts = cp.getProperty("timestamp");
                if (ts == null || ts.getType() != LONG || now > ts.getValue(LONG)) {
                    cp.remove();
                }
            }

            NodeBuilder cp = checkpoints.child(name);
            if (Long.MAX_VALUE - now > lifetime) {
                cp.setProperty("timestamp", now + lifetime);
            } else {
                cp.setProperty("timestamp", Long.MAX_VALUE);
            }
            cp.setProperty("created", now);

            NodeBuilder props = cp.setChildNode("properties");
            for (Entry<String, String> p : properties.entrySet()) {
                props.setProperty(p.getKey(), p.getValue());
            }
            cp.setChildNode(ROOT, state.getChildNode(ROOT));

            SegmentNodeState newState = builder.getNodeState();
            if (revisions.setHead(state.getRecordId(), newState.getRecordId())) {
                refreshHead(false);
                return true;
            } else {
                return false;
            }
        }
    }
    
    /**
     * Sets the number of seconds to wait for the attempt to grab the lock to
     * create a checkpoint
     */
    void setCheckpointsLockWaitTime(int checkpointsLockWaitTime) {
        this.checkpointsLockWaitTime = checkpointsLockWaitTime;
    }
    
    public ChangeDispatcher changeDispatcher() {
        return changeDispatcher;
    }
}
