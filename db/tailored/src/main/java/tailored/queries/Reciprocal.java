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

public class Reciprocal implements Workload {
  private final BenchmarkContext ctx;
  private long[] startCandidateIds;
  private long[] endCandidateIds;

  public Reciprocal(BenchmarkContext ctx) throws Exception {
    this.ctx = ctx;

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
      executePostgres(iteration);
    } else if (ctx.config.dbms() == Dbms.NEO4J) {
      executeNeo4j(iteration);
    }
  }

  private void executePostgres(int iteration) throws Exception {
    String sql = """
        SELECT
          EXISTS (
            SELECT 1 FROM edges WHERE start_id = ? AND end_id = ?
          ) AS forward_exists,
          EXISTS (
            SELECT 1 FROM edges WHERE start_id = ? AND end_id = ?
          ) AS backward_exists
        """;

    assert ctx.pgConn != null;
    try (PreparedStatement ps = ctx.pgConn.prepareStatement(sql)) {
      long u = startCandidateIds[iteration];
      long v = endCandidateIds[iteration];

      ps.setLong(1, u);
      ps.setLong(2, v);
      ps.setLong(3, v);
      ps.setLong(4, u);

      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
    }
  }

  private void executeNeo4j(int iteration) throws Exception {
    String cypher = """
          MATCH (u:Person {id: $startId})-[:FRIENDS_WITH]->(v:Person {id: $endId}),
                (v)-[:FRIENDS_WITH]->(u)
          RETURN u.id as startId, v.id as endId
        """;

    long u = startCandidateIds[iteration];
    long v = endCandidateIds[iteration];

    assert ctx.neoDriver != null;
    EagerResult rs = ctx.neoDriver.executableQuery(cypher)
        .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
        .withParameters(Map.of("startId", u, "endId", v))
        .execute();

    rs.records().size();
  }

  private long[][] genPairsPostgres(Connection conn, int operations) throws Exception {
    String sql = "SELECT DISTINCT id FROM nodes ORDER BY random() LIMIT " + operations * 2;

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
