/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.scalability.benchmarks.segment.standby;

import java.util.Random;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark;
import org.apache.jackrabbit.oak.scalability.suites.ScalabilityAbstractSuite.ExecutionContext;

public class BulkTransferBenchmark extends ScalabilityBenchmark {
    private static final int NODE_COUNT = Integer.getInteger("nodeCount", 100_000);

    @Override
    public void execute(Repository repository, Credentials credentials, ExecutionContext context) throws Exception {
        Session session = repository.login(credentials);
        Node root = session.getRootNode().addNode("root", "nt:folder");
        createNodes(root.addNode("store"), NODE_COUNT, new Random());
        session.save();
    }
    
    private static void createNodes(Node parent, int nodeCount, Random random) throws Exception {
        for (int j = 0; j <= nodeCount / 1000; j++) {
            Node folder = parent.addNode("Folder#" + j);
            for (int i = 0; i < (nodeCount < 1000 ? nodeCount : 1000); i++) {
                folder.addNode("Test#" + i).setProperty("ts", random.nextLong());
            }
        }
    }

//    private void test(String name, int nodeCount, boolean useSSL) throws Exception {
//        try (StandbyServerSync serverSync = new StandbyServerSync(PORT, primaryStore, 1024 * 1024, useSSL);
//             StandbyClientSync clientSync = new StandbyClientSync(HOST, PORT, standbyStore, useSSL, TIMEOUT, false)) {
//            serverSync.start();
//
//            MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
//            ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
//            ObjectName clientStatus = new ObjectName(clientSync.getMBeanName());
//            ObjectName serverStatus = new ObjectName(serverSync.getMBeanName());
//
//            Stopwatch stopwatch = Stopwatch.createStarted();
//            clientSync.run();
//            stopwatch.stop();
//
//            Set<ObjectName> instances = jmxServer.queryNames(status, null);
//            ObjectName connectionStatus = null;
//            for (ObjectName s : instances) {
//                if (!s.equals(clientStatus) && !s.equals(serverStatus)) {
//                    connectionStatus = s;
//                }
//            }
//            assert (connectionStatus != null);
//
//            long segments = (Long) jmxServer.getAttribute(connectionStatus, "TransferredSegments");
//            long bytes = (Long) jmxServer.getAttribute(connectionStatus, "TransferredSegmentBytes");
//
//            System.out.printf("%s: segments = %d, segments size = %d bytes, time = %s\n", name, segments, bytes, stopwatch);
//        }
//    }
}
