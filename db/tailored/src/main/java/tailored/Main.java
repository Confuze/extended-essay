package tailored;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

// parsing arguments and creating all classes, running workload here
public class Main {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Not enough arguments.\nUsage: <command> <dbms> [...args]");
            System.exit(1);
        }
        String command = args[0].toLowerCase();
        Dbms dbms = parseDbms(args[1]);

        switch (command) {
            case "load" -> {
                if (args.length < 4) {
                    System.err.println("Not enough arguments.\nUsage: load <dbms> <nodes_path> <edges_path");
                    System.exit(1);
                }

                Path nodesPath = Paths.get(args[2]);
                Path edgesPath = Paths.get(args[3]);

                Loader.runLoad(dbms, nodesPath, edgesPath);
            }
            case "run" -> {
                if (args.length < 6) {
                    System.err.println("Not enough arguments.\nUsage: run <dbms> <workload> <depth> <operation_count> <threads> <output_path>\nUse depth = 1 for workloads that don't require it.\noutput_path is optional. If null, output is printed in console");
                    System.exit(1);
                }

                WorkloadType workloadType = parseWorkload(args[2]);
                int depth = Integer.parseInt(args[3]);
                int operationCount = Integer.parseInt(args[4]);
                int threads = Integer.parseInt(args[5]);
                Path outputPath = args.length >= 7 ? Paths.get(args[6]) : null;

                BenchmarkConfig config = BenchmarkConfig.create(dbms, workloadType, depth, operationCount, threads, outputPath);

                try (BenchmarkContext ctx = ConnectionFactory.openContext(config)) {
                    Workload workload = WorkloadFactory.create(ctx);
                    WorkloadRunner runner = new WorkloadRunner();

                    RunResult res = runner.run(workload, ctx, operationCount, threads);

                    if (outputPath != null) {
                        ResultWriter.write(outputPath, config, res);
                    } else {
                        System.out.println(res);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            default -> System.err.println("Unknown command: " + command + "\n Use load or run");
        }
    }

    private static WorkloadType parseWorkload(String arg) {
        WorkloadType workload = null;

        try {
            workload = WorkloadType.valueOf(arg.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid workload argument.\nAvailable options:\n" + Arrays.toString(WorkloadType.values()));
            System.exit(1);
        }

        return workload;
    }

    private static Dbms parseDbms(String arg) {
        Dbms dbms = null;

        try {
            dbms = Dbms.valueOf(arg.toUpperCase());
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid dbms argument.\nAvailable options:\n" + Arrays.toString(Dbms.values()));
            System.exit(1);
        }

        return dbms;
    }
}