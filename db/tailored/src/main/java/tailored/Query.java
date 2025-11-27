// interface for query files

package tailored;

public interface Query {
  void executeOnce(BenchmarkContext ctx) throws Exception;
}