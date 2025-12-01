package tailored;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public final class WorkloadRunner {

  public RunResult run(Workload workload,
                       BenchmarkContext ctx,
                       int operations,
                       int threads) throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(threads);
    long[] latenciesNs = new long[operations];
    AtomicInteger index = new AtomicInteger(0);
    AtomicInteger succeeded = new AtomicInteger(0);
    AtomicInteger failed = new AtomicInteger(0);

    int base = operations / threads;
    int rem = operations % threads;

    long startWall = System.nanoTime();

    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      int opsThis = base + (t < rem ? 1 : 0);
      futures.add(exec.submit(() -> {
        for (int i = 0; i < opsThis; i++) {
          int idx = index.getAndIncrement();
          long start = System.nanoTime();
          try {
            workload.executeOnce(ctx, idx);
            succeeded.incrementAndGet();
          } catch (Exception e) {
            failed.incrementAndGet();
          }
          long end = System.nanoTime();
          latenciesNs[idx] = end - start;
        }
      }));
    }
    for (Future<?> f : futures) f.get();
    exec.shutdown();

    long endWall = System.nanoTime();
    long totalTimeMs = (endWall - startWall) / 1_000_000L;
    int n = index.get();

    long[] sorted = Arrays.copyOf(latenciesNs, n);
    Arrays.sort(sorted);

    long p95Ns = sorted[(int) Math.floor(0.95 * n) - 1];
    long p99Ns = sorted[(int) Math.floor(0.99 * n) - 1];

    long sumNs = 0;
    for (int i = 0; i < n; i++) sumNs += sorted[i];

    double avgUs = sumNs / 1000.0 / n;
    double throughput = n * 1000.0 / totalTimeMs;

    return new RunResult(
        n,
        totalTimeMs,
        throughput,
        avgUs,
        p95Ns / 1000,
        p99Ns / 1000,
        succeeded.get(),
        failed.get()
    );
  }
}