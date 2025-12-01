package tailored.queries;

import org.neo4j.driver.Driver;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.QueryConfig;
import tailored.BenchmarkContext;
import tailored.Dbms;
import tailored.Workload;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Filtered implements Workload {
  private long[] candidateIds;

  public Filtered(BenchmarkContext ctx) throws Exception {
    if (ctx.config.dbms() == Dbms.POSTGRES) {
      this.candidateIds = genIdsPostgres(ctx.pgConn, ctx.config.operations());
    } else if (ctx.config.dbms() == Dbms.NEO4J) {
      this.candidateIds = genIdsNeo4j(ctx.neoDriver, ctx.config.operations());
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
            SELECT DISTINCT n.id
            FROM edges e
            JOIN nodes n ON n.id = e.end_id
            WHERE e.start_id = ?
              AND n.age > 18
              AND n.age < 25;
        """;

    assert ctx.pgConn != null;
    try (PreparedStatement ps = ctx.pgConn.prepareStatement(sql)) {
      long startId = candidateIds[iteration];
      ps.setLong(1, startId);

      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
      }
    }
  }

  private void executeNeo4j(BenchmarkContext ctx, int iteration) throws Exception {
    String cypher = """
                    MATCH (u:Person {id: $startId})-[:FRIENDS_WITH]->(v:Person)
                    WHERE v.age > 18 AND v.age < 25
                    RETURN v.id AS nodeId;
                """;

    assert ctx.neoDriver != null;
    EagerResult rs = ctx.neoDriver.executableQuery(cypher)
        .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
        .withParameters(Map.of("startId", candidateIds[iteration])).execute();

    rs.records().size();
  }

  private long[] genIdsPostgres(Connection conn, int operations) {
    String sql = "SELECT DISTINCT start_id FROM edges TABLESAMPLE SYSTEM (20)";

    try (Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(sql)) {
      List<Long> ids = new ArrayList<>(operations);
      while (rs.next()) {
        ids.add(rs.getLong(1));
      }

      return ids.stream().mapToLong(Long::longValue).toArray();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private long[] genIdsNeo4j(Driver driver, int operations) {
    String cypher = "MATCH (p:Person)-[:FRIENDS_WITH]->() WITH DISTINCT p ORDER BY rand() LIMIT " + operations + " return p.id as id";

    try {
      EagerResult rs = driver.executableQuery(cypher)
          .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
          .execute();

      return rs.records().stream().mapToLong(r -> r.get("id").asLong())
          .toArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}