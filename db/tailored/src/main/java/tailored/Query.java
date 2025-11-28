// interface for query files

package tailored;

public interface Query {
  void executeOnce(BenchmarkContext ctx, int iteration) throws Exception;
}