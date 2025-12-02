package tailored;

import tailored.workloads.FriendOfFriend;
import tailored.workloads.Reciprocal;
import tailored.workloads.ShortestPath;
import tailored.workloads.Filtered;


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