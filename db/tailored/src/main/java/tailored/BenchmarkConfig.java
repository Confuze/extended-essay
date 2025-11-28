// record with all config options passed via arguments + env vars

package tailored;

import reactor.util.annotation.Nullable;

public record BenchmarkConfig (
    Dbms dbms,
    WorkloadType workloadType,
    int depth,
    int operations,
    int threads,
    @Nullable String outputPath,
    @Nullable String pgUrl,
    @Nullable String pgUser,
    @Nullable String pgPassword,
    @Nullable String neo4jUri,
    @Nullable String neo4jUser,
    @Nullable String neo4jPassword
) {
    public static BenchmarkConfig create(Dbms dbms,
                           WorkloadType type,
                           int depth,
                           int operations,
                           int threads,
                           String outputPath) {


        String pgUri = System.getenv("PG_URI");
        String pgUser = System.getenv("PG_USER");
        String pgPass = System.getenv("PG_PASS");

        String neoUri = System.getenv("NEO4J_URI");
        String neoUser = System.getenv("NEO4J_USER");
        String neoPass = System.getenv("NEO4J_PASS");

        if (dbms == Dbms.POSTGRES) {
            if (pgUri == null || pgUser == null || pgPass == null) {
                throw new IllegalStateException("environment variables missing");
            }

            return new BenchmarkConfig(dbms, type, depth, operations, threads, outputPath,
                    pgUri, pgUser, pgPass,
                    null, null, null);
        } else if (dbms == Dbms.NEO4J) {
            if (neoUri == null || neoUser == null || neoPass == null) {
                throw new IllegalStateException("environment variables missing");
            }

            return new BenchmarkConfig(dbms, type, depth, operations, threads, outputPath,
                    null, null, null,
                    neoUri, neoUser, neoPass);
        } else {
            throw new IllegalStateException("unknown Dbms type");
        }
    }
}