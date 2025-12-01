package tailored;

public record RunResult(
    int totalOps,
    long totalTimeMs,
    double throughputOpsPerSec,
    double avgLatencyUs,
    long p95LatencyUs,
    long p99LatencyUs,
    int succeededCount,
    int failedCount
) {}