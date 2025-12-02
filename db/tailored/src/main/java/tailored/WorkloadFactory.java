package tailored;

import tailored.queries.FriendOfFriend;
import tailored.queries.Reciprocal;
import tailored.queries.ShortestPath;
import tailored.queries.Filtered;


public class WorkloadFactory {

  public static Workload create(BenchmarkContext ctx) throws Exception {
    return switch (ctx.config.workloadType()) {
      case FOF              -> new FriendOfFriend(ctx);
      case RECIPROCAL       -> new Reciprocal(ctx);
      case SHORTEST_PATH    -> new ShortestPath(ctx);
      case FILTERED         -> new Filtered(ctx);
    };
  }
}