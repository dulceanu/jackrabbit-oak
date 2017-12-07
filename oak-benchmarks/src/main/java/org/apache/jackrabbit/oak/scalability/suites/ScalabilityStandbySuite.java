package org.apache.jackrabbit.oak.scalability.suites;

import javax.jcr.Repository;
import org.apache.jackrabbit.oak.fixture.OakFixture;
import org.apache.jackrabbit.oak.fixture.OakRepositoryFixture;
import org.apache.jackrabbit.oak.fixture.RepositoryFixture;
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

    @Override
    public ScalabilitySuite addBenchmarks(ScalabilityBenchmark... benchmarks) {
        for (ScalabilityBenchmark sb : benchmarks) {
            this.benchmarks.put(sb.toString(), sb);
        }
        return this;
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

    @Override
    protected Repository[] createRepository(RepositoryFixture fixture) throws Exception {
        if (fixture instanceof OakRepositoryFixture) {
            OakRepositoryFixture orf = (OakRepositoryFixture) fixture;
            if (orf.getOakFixture().toString().equals(OakFixture.OAK_SEGMENT_TAR_COLD)) {
                return super.createRepository(fixture);
            }
        }

        throw new IllegalArgumentException(
                "Cannot run ScalabilityStandbySuite on current fixture. Use Oak-Segment-Tar-Cold instead!");
    }
}
