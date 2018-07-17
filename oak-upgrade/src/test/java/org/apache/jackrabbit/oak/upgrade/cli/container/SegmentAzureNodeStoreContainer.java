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
package org.apache.jackrabbit.oak.upgrade.cli.container;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class SegmentAzureNodeStoreContainer implements NodeStoreContainer {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentAzureNodeStoreContainer.class);

    private final String dir;

    private final BlobStoreContainer blob;

    private final CloudBlobContainer container;

    private FileStore fs;

    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    public SegmentAzureNodeStoreContainer() throws Exception {
        this(null, null);
    }

    public SegmentAzureNodeStoreContainer(String dir) throws Exception {
        this(null, dir);
    }

    public SegmentAzureNodeStoreContainer(BlobStoreContainer blob) throws Exception {
        this(blob, null);
    }

    private SegmentAzureNodeStoreContainer(BlobStoreContainer blob, String dir) throws Exception {
        this.blob = blob;
        this.dir = dir == null ? "repository" : dir;
        this.container = azurite.getContainer("oak-test");
    }

    @Override
    public NodeStore open() throws IOException {
        AzurePersistence azPersistence = null;
        try {
            azPersistence = new AzurePersistence(container.getDirectoryReference(dir));
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }

        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(Files.createTempDir())
                .withCustomPersistence(azPersistence).withMemoryMapping(false);

        if (blob != null) {
            builder.withBlobStore(blob.open());
        }

        try {
            fs = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        return SegmentNodeStoreBuilders.builder(fs).build();
    }

    @Override
    public void close() {
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    @Override
    public void clean() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public String getDescription() {
        String description = "AzureSegmentStore@";
        try {
            description += container.getDirectoryReference(dir).getUri().toString();
        } catch (URISyntaxException e) {
            LOG.error("Can't obtain directory reference " + dir + " for container " + container, e);
        }

        return description;
    }

}
