package com.csforge.sstable;

import com.google.common.collect.Lists;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.commons.cli.*;

import java.io.*;
import java.util.List;

public class Cqlsh {

    private static final Options options = new Options();

    private static final String SCHEMA_OPTION = "s";

    private static final String FILE_OPTION = "f";

    static {
        Option schemaOption = new Option(SCHEMA_OPTION, true, "Schema file to use.");
        schemaOption.setRequired(true);

        Option fileOption = new Option(FILE_OPTION, true, "Execute commands from FILE, then exit.");

        options.addOption(schemaOption);
        options.addOption(fileOption);
    }

    public static void main(String args[]) {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.getArgs().length == 0) {
                throw new ParseException("You must supply at least one sstable.");
            }
        } catch (ParseException e) {
            System.err.format("Failure parsing arguments: %s%n%n", e.getMessage());
            try (PrintWriter errWriter = new PrintWriter(System.err, true)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(errWriter, 120, "cqlsh sstable [sstable ...]",
                        String.format("%nOffline CQL Shell for Apache Cassandra 3.x%nOptions:"),
                        options, 2, 1, "", true);
            } finally {
                System.exit(-1);
            }
        }

        List<File> sstables = Lists.newArrayList();
        for (String sstable : cmd.getArgs()) {
            File file = new File(sstable);
            if (!file.exists()) {
                System.err.println("Non-existant sstable file provided: " + sstable);
                System.exit(-3);
            }
            sstables.add(file);
        }
        String schemaPath = cmd.getOptionValue(SCHEMA_OPTION);
        String cqlFilePath = cmd.getOptionValue(FILE_OPTION);
        CFMetaData metadata = null;

        try (InputStream schemaStream = new FileInputStream(new File(schemaPath))) {
            metadata = CassandraUtils.tableFromCQL(schemaStream);
        } catch (IOException e) {
            System.err.println("Error reading schema metadata: " + e.getMessage());
            System.exit(-2);
        }

        try {
            ConsoleReader console = new ConsoleReader();
            console.setPrompt("cqlsh> ");
            FileHistory history = new FileHistory(new File(System.getProperty("user.home"), ".sstable-tools-cqlsh"));
            console.setHistory(history);
            console.setHistoryEnabled(true);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        history.flush();
                    } catch (IOException e) {
                        System.err.println("Couldn't flush history on shutdown.  Reason:" + e.getMessage());
                    }
                }
            });

            try {
                String line;
                boolean done = false;
                while (!done && (line = console.readLine()) != null) {
                    // TODO: Handle semi-colons in quoted identifers.
                    String cmds[] = line.split(";");
                    for (String command : cmds) {
                        command = command.trim();
                        if (command.equals("exit")) {
                            done = true;
                            continue;
                        } else if (command.length() >= 6) {
                            String queryType = command.substring(0, 6).toLowerCase();
                            switch (queryType) {
                                case "select":
                                    try {
                                        Query query = new Query(command, sstables.get(0), metadata);
                                        TableTransformer.dumpResults(metadata, query.getResults(), System.out);
                                    } catch (Exception e) {
                                        System.err.println(e.getMessage());
                                    }
                                    continue;
                                case "update":
                                case "insert":
                                case "create":
                                case "delete":
                                    System.err.format("Query '%s' is not supported since this tool is read-only.%n", command);
                                    continue;
                            }
                        }
                        System.err.format("Unknown command: %s%n", command);
                    }
                }
            } finally {
                history.flush();
                console.shutdown();
            }
        } catch (IOException e) {
            System.err.println("Error in console session: " + e.getMessage());
            System.exit(-4);
        }

    }
}
