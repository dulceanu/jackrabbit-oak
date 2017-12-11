package org.apache.jackrabbit.oak.scalability.suites;

import java.io.ByteArrayInputStream;
import java.util.Calendar;
import java.util.Map;
import java.util.Random;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
import org.apache.jackrabbit.oak.fixture.SegmentTarFixture;
import org.apache.jackrabbit.oak.scalability.ScalabilitySuite;
import org.apache.jackrabbit.oak.scalability.benchmarks.ScalabilityBenchmark;

/**
 * TODO: Add JavaDoc
 *
 */
public class ScalabilityStandbySuite extends ScalabilityAbstractSuite {
    /**
     * Add JavaDoc
     */
    private static final boolean SECURE = Boolean.getBoolean("secure");

    /**
     * Add JavaDoc
     */
    private static final int NODE_COUNT = Integer.getInteger("nodeCount", 100_000);

    /**
     * Iteration counter
     */
    private int iteration = 0;

    @Override
    public ScalabilitySuite addBenchmarks(ScalabilityBenchmark... benchmarks) {
        for (ScalabilityBenchmark sb : benchmarks) {
            this.benchmarks.put(sb.toString(), sb);
        }
        return this;
    }

    @Override
    public void setUp(Repository repository, RepositoryFixture fixture, Credentials credentials) throws Exception {
        super.setUp(repository, fixture, credentials);

        if (!(fixture instanceof OakRepositoryFixture)) {
            return;
        }

        OakRepositoryFixture orf = (OakRepositoryFixture) fixture;
        SegmentTarFixture stf = (SegmentTarFixture) orf.getOakFixture();

        if (orf.toString().equals(OakFixture.OAK_SEGMENT_TAR_COLD)) {

            Map<Object, Object> contextMap = context.getMap();
            contextMap.put("clientSyncs", stf.getClientSyncs());
            contextMap.put("serverSyncs", stf.getServerSyncs());
            contextMap.put("stores", stf.getStores());
        } else {
            throw new IllegalArgumentException(
                    "Cannot run ScalabilityStandbySuite on current fixture. Use Oak-Segment-Tar-Cold instead!");
        }
    }

    @Override
    public void beforeIteration(ExecutionContext context) throws Exception {
        Session session = loginWriter();
        Node rootFolder = session.getRootNode().addNode("rootFolder" + iteration++, "nt:folder");
        createNodes(rootFolder, NODE_COUNT, new Random());
        session.save();
    }

    @Override
    protected void executeBenchmark(ScalabilityBenchmark benchmark, ExecutionContext context) throws Exception {
        LOG.info("Started pre benchmark hook : {}", benchmark);
        benchmark.beforeExecute(getRepository(), CREDENTIALS, context);

        LOG.info("Started execution : {}", benchmark);
        if (PROFILE) {
            context.startProfiler();
        }

        try {
            benchmark.execute(getRepository(), CREDENTIALS, context);
        } catch (Exception e) {
            LOG.error("Exception in benchmark execution ", e);
        }

        context.stopProfiler();

        LOG.info("Started post benchmark hook : {}", benchmark);
        benchmark.afterExecute(getRepository(), CREDENTIALS, context);
    }

    @SuppressWarnings("deprecation")
    private static void createNodes(Node parent, int nodeCount, Random random) throws Exception {
        final int blobSize = 5 * 1024;

        for (int j = 0; j <= nodeCount / 1000; j++) {
            Node folder = parent.addNode("Folder#" + j, "nt:folder");
            for (int i = 0; i < (nodeCount < 1000 ? nodeCount : 1000); i++) {
                Node file = folder.addNode("server" + i, "nt:file");

                byte[] data = new byte[blobSize];
                new Random().nextBytes(data);

                Node content = file.addNode("jcr:content", "nt:resource");
                content.setProperty("jcr:mimeType", "application/octet-stream");
                content.setProperty("jcr:lastModified", Calendar.getInstance());
                content.setProperty("jcr:data", new ByteArrayInputStream(data));
            }
        }
    }
}
