// interface for query files

package tailored;

public interface Workload {
  void executeOnce(BenchmarkContext ctx, int iteration) throws Exception;
}