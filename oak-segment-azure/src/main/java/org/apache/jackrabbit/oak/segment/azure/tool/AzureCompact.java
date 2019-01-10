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

package org.apache.jackrabbit.oak.segment.azure.tool;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printableStopwatch;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.createCloudBlobDirectory;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.deleteAllEntries;

import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Stopwatch;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.tool.Compact;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Perform an offline compaction of an existing Azure Segment Store.
 */
public class AzureCompact {

    /**
     * Create a builder for the {@link Compact} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Compact} command.
     */
    public static class Builder {

        private String path;

        private long gcLogInterval = 150000;

        private int segmentCacheSize = DEFAULT_SEGMENT_CACHE_MB;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path (URI) to an existing segment store. This parameter is required.
         *
         * @param path
         *            the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(String path) {
            this.path = checkNotNull(path);
            return this;
        }

        /**
         * The size of the segment cache in MB. The default of
         * {@link SegmentCache#DEFAULT_SEGMENT_CACHE_MB} when this method is not
         * invoked.
         *
         * @param segmentCacheSize
         *            cache size in MB
         * @return this builder
         * @throws IllegalArgumentException
         *             if {@code segmentCacheSize} is not a positive integer.
         */
        public Builder withSegmentCacheSize(int segmentCacheSize) {
            checkArgument(segmentCacheSize > 0, "segmentCacheSize must be strictly positive");
            this.segmentCacheSize = segmentCacheSize;
            return this;
        }

        /**
         * The number of nodes after which an update about the compaction process is
         * logged. Set to a negative number to disable progress logging. If not
         * specified, it defaults to 150,000 nodes.
         *
         * @param gcLogInterval
         *            The log interval.
         * @return this builder.
         */
        public Builder withGCLogInterval(long gcLogInterval) {
            this.gcLogInterval = gcLogInterval;
            return this;
        }

        /**
         * Create an executable version of the {@link Compact} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public AzureCompact build() {
            checkNotNull(path);
            return new AzureCompact(this);
        }
    }

    private final String path;

    private final int segmentCacheSize;

    private final long gcLogInterval;

    private AzureCompact(Builder builder) {
        this.path = builder.path;
        this.segmentCacheSize = builder.segmentCacheSize;
        this.gcLogInterval = builder.gcLogInterval;
    }

    public int run() {
        PrintWriter outWriter = new PrintWriter(System.out, true);
        PrintWriter errWriter = new PrintWriter(System.err, true);
        Closer closer = Closer.create();

        try {
            File localSegmentStoreDir = createTempDir(closer);

            System.out.printf("Downloading %s to local repository \n", path);
            Stopwatch watch = Stopwatch.createStarted();
            int returnCode = migrateSegmentStore(path, localSegmentStoreDir.getAbsolutePath(), outWriter, errWriter);

            if (returnCode != 0) {
                return 1;
            }

            watch.stop();
            System.out.printf("Download took %s \n", printableStopwatch(watch));

            System.out.printf("Compacting nodes locally... \n");
            returnCode = Compact.builder().withMmap(false).withSegmentCacheSize(segmentCacheSize)
                    .withGCLogInterval(gcLogInterval).withOs(StandardSystemProperty.OS_NAME.value())
                    .withPath(localSegmentStoreDir).build().run();

            if (returnCode != 0) {
                return 1;
            }

            watch = Stopwatch.createStarted();
            System.out.printf("Cleaning up obsolete entries from remote repository %s \n", path);
            CloudBlobDirectory directory = createCloudBlobDirectory(path.substring(3));
            deleteAllEntries(directory);
            watch.stop();
            System.out.printf("Cleanup took %s \n", printableStopwatch(watch));

            watch = Stopwatch.createStarted();
            System.out.printf("Uploading local repository to %s \n", path);
            returnCode = migrateSegmentStore(localSegmentStoreDir.getAbsolutePath(), path, outWriter, errWriter);

            if (returnCode != 0) {
                return 1;
            }

            watch.stop();
            System.out.printf("Upload took %s \n", printableStopwatch(watch));

            return 0;
        } catch (IOException e) {
            System.out.printf("Failed to clean up obsolete entries from remote repository % \n", path);
            errWriter.print(e);
            return 1;
        } finally {
            try {
                closer.close();
            } catch (IOException e) {
                errWriter.print(e);
            }
        }
    }

    private int migrateSegmentStore(String sourcePath, String targetPath, PrintWriter outWriter, PrintWriter errWriter) {
        SegmentCopy segmentCopy = SegmentCopy.builder().withSource(sourcePath)
                .withDestination(targetPath).withOutWriter(outWriter).withErrWriter(errWriter)
                .build();
        return segmentCopy.run();
    }

    private static File createTempDir(Closer closer) {
        File dir = Files.createTempDir();
        closer.register(() -> FileUtils.deleteDirectory(dir));
        return dir;
    }
}
