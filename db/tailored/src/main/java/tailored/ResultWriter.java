// writes results to disk based on filepath passed as argument

package tailored;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class ResultWriter {
    private ResultWriter() {}

    public static void write(Path path, BenchmarkConfig config, RunResult result) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(path)) {
            w.write("DBMS=" + config.dbms());
            w.newLine();
            w.write("WORKLOAD=" + config.workloadType());
            w.newLine();
            w.write("DEPTH=" + config.depth());
            w.newLine();
            w.write("OPERATIONS=" + result.totalOps());
            w.newLine();
            w.newLine();

            w.write("OVERALL RunTime(ms)=" + result.totalTimeMs());
            w.newLine();
            w.write("OVERALL Throughput(ops/sec)=" + String.format("%.2f", result.throughputOpsPerSec()));
            w.newLine();
            w.newLine();

            w.write("LATENCY AvgLatency(us)=" + String.format("%.2f", result.avgLatencyUs()));
            w.newLine();
            w.write("LATENCY P95Latency(us)=" + result.p95LatencyUs());
            w.newLine();
            w.write("LATENCY P99Latency(us)=" + result.p99LatencyUs());
            w.newLine();
            w.newLine();

            w.write("STATUS Succeeded=" + result.succeededCount());
            w.newLine();
            w.write("STATUS Failed=" + result.failedCount());
            w.newLine();
        }
    }
}
