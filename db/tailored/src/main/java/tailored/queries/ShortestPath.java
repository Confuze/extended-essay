package tailored.queries;

import org.neo4j.driver.Driver;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.QueryConfig;
import tailored.BenchmarkContext;
import tailored.Dbms;
import tailored.Workload;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ShortestPath implements Workload {
  private long[] startCandidateIds;
  private long[] endCandidateIds;

  public ShortestPath(BenchmarkContext ctx) throws Exception {
    if (ctx.config.dbms() == Dbms.POSTGRES) {
      long[][] pairs = genPairsPostgres(ctx.pgConn, ctx.config.operations());
      this.startCandidateIds = pairs[0];
      this.endCandidateIds   = pairs[1];
    } else if (ctx.config.dbms() == Dbms.NEO4J) {
      assert ctx.neoDriver != null;
      long[][] pairs = genPairsNeo4j(ctx.neoDriver, ctx.config.operations());
      this.startCandidateIds = pairs[0];
      this.endCandidateIds = pairs[1];
    }
  }

  @Override
  public void executeOnce(BenchmarkContext ctx, int iteration) throws Exception {
    if (ctx.config.dbms() == Dbms.POSTGRES) {
      executePostgres(ctx, iteration);
    } else if (ctx.config.dbms() == Dbms.NEO4J) {
      executeNeo4j(ctx, iteration);
    }
  }

  private void executePostgres(BenchmarkContext ctx, int iteration) throws Exception {
    String sql = """
        WITH RECURSIVE bfs AS (
            SELECT
                0 AS depth,
                ? AS node_id
            UNION ALL
            SELECT
                bfs.depth + 1,
                e.end_id
            FROM bfs
            JOIN edges e ON e.start_id = bfs.node_id
            WHERE bfs.depth < ?
        )
        SELECT MIN(depth) AS dist
        FROM bfs
        WHERE node_id = ?;
        """;

    assert ctx.pgConn != null;
    try (PreparedStatement ps = ctx.pgConn.prepareStatement(sql)) {
      long startId = startCandidateIds[iteration];
      long endId   = endCandidateIds[iteration];

      ps.setLong(1, startId);
      ps.setInt(2, ctx.config.depth()); // max depth
      ps.setLong(3, endId);

      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
    }
  }

  private void executeNeo4j(BenchmarkContext ctx, int iteration) throws Exception {
    long startId = startCandidateIds[iteration];
    long endId   = endCandidateIds[iteration];

    String cypher =
        "MATCH (u:Person {id: $u}), (v:Person {id: $v}) " +
            "MATCH p = shortestPath((u)-[:FRIENDS_WITH*.." + ctx.config.depth() + "]->(v)) " +
            "RETURN length(p) AS dist";

    assert ctx.neoDriver != null;
    EagerResult rs = ctx.neoDriver.executableQuery(cypher)
        .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
        .withParameters(Map.of("u", startId, "v", endId))
        .execute();

    rs.records().size();
  }


  private long[][] genPairsPostgres(Connection conn, int operations) throws Exception {
    String sql = "SELECT start_id FROM (SELECT DISTINCT start_id FROM edges) as S ORDER BY random() LIMIT " + operations * 2;

    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(sql)) {

      List<Long> ids = new ArrayList<>(operations * 2);
      while (rs.next()) {
        ids.add(rs.getLong(1));
      }

      long[] startArr = new long[operations];
      long[] endArr = new long[operations];

      for (int i = 0; i < operations; i++) {
        startArr[i] = ids.get(2 * i);
        endArr[i]   = ids.get(2 * i + 1);
      }

      return new long[][]{startArr, endArr};
    }
  }

  private long[][] genPairsNeo4j(Driver driver, int operations) {
    String cypher =
        "MATCH (p:Person) " +
            "RETURN p.id as id " +
            "ORDER BY rand() LIMIT " + operations * 2;

    EagerResult rs = driver.executableQuery(cypher)
        .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
        .execute();

    List<Long> ids = new ArrayList<>(operations * 2);
    rs.records().forEach(rec -> ids.add(rec.get("id").asLong()));

    long[] startArr = new long[operations];
    long[] endArr = new long[operations];

    for (int i = 0; i < operations; i++) {
      startArr[i] = ids.get(2 * i);
      endArr[i]   = ids.get(2 * i + 1);
    }

    return new long[][]{startArr, endArr};
  }
}
