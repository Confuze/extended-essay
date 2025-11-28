package tailored.queries;

import org.neo4j.driver.Driver;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.QueryConfig;
import tailored.BenchmarkContext;
import tailored.Dbms;
import tailored.Query;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FriendOfFriend implements Query {
    private BenchmarkContext ctx;
    private long[] candidateIds;

    public FriendOfFriend(BenchmarkContext ctx) throws Exception {
        this.ctx = ctx;

        if (ctx.config.dbms() == Dbms.POSTGRES) {
            this.candidateIds = genIdsPostgres(ctx.pgConn, ctx.config.operations());
        } else if (ctx.config.dbms() == Dbms.NEO4J) {
            this.candidateIds = genIdsNeo4j(ctx.neoDriver, ctx.config.operations());
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
        
            SELECT DISTINCT node_id
            FROM bfs
            WHERE depth = ?;
        """;

        assert ctx.pgConn != null;
        try (PreparedStatement ps = ctx.pgConn.prepareStatement(sql)) {
            long startId = candidateIds[iteration];
            ps.setLong(1, startId);
            ps.setInt(2, ctx.config.depth());
            ps.setInt(3, ctx.config.depth());

            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
            }
        }
    }

    private void executeNeo4j(int iteration) throws Exception {
        String cypher = """
                    MATCH p = (start:Person {id: $startId})-[:FRIENDS_WITH*1..5]->(fof)
                    WHERE length(p) = $depth
                    RETURN DISTINCT fof.id AS nodeId;
                """;

        assert ctx.neoDriver != null;
        EagerResult rs = ctx.neoDriver.executableQuery(cypher)
                .withConfig(QueryConfig.builder().withDatabase("neo4j").build())
                .withParameters(Map.of("startId", candidateIds[iteration], "depth", ctx.config.depth())).execute();

        rs.records().getFirst();
    }

    private long[] genIdsPostgres(Connection conn, int operations) {
        String sql = "SELECT DISTINCT start_id FROM edges ORDER BY random() LIMIT " + operations;

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
