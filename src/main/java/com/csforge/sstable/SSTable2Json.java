package com.csforge.sstable;

import java.io.PrintWriter;

import org.apache.cassandra.config.Config;
import org.apache.commons.cli.*;

public class SSTable2Json {

    private static final Options options = new Options();

    private static final String PARTITION_KEY_OPTION = "k";

    private static final String EXCLUDE_KEY_OPTION = "x";

    private static final String CONFIG_YAML_OPTION = "c";

    private static final String ENUMERATE_KEYS_OPTION = "e";

    static {
        Config.setClientMode(true);

        Option partitionKey = new Option(PARTITION_KEY_OPTION, true, "Partition Key");
        partitionKey.setArgs(Option.UNLIMITED_VALUES);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "Excluded Partition Key");
        excludeKey.setArgs(Option.UNLIMITED_VALUES);

        Option enumerateKeys = new Option(ENUMERATE_KEYS_OPTION, false, "Enumerate keys only");

        Option conf = new Option(CONFIG_YAML_OPTION, true, "cassandra.yaml configuration file.");


        options.addOption(partitionKey);
        options.addOption(excludeKey);
        options.addOption(enumerateKeys);
        options.addOption(conf);
    }

    public static void main(String args[]) {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.getArgs().length != 1) {
                throw new ParseException("You must supply exactly one sstable." + cmd.getArgs().length);
            }
        } catch (ParseException e) {
            System.err.println("Failure parsing arguments: " + e.getMessage());
            try (PrintWriter errWriter = new PrintWriter(System.err, true)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(errWriter, 120, "sstable2json <sstable>",
                    "Converts on-disk SSTable representation of a table into a JSON formatted document.",
                    options, 2, 1, "", true);
            } finally {
                System.exit(-1);
            }
        }

        String sstablePath = cmd.getArgs()[0];
        String[] keys = cmd.getOptionValues(PARTITION_KEY_OPTION);
        String[] excludes = cmd.getOptionValues(EXCLUDE_KEY_OPTION);
        boolean enumerateKeysOnly = cmd.hasOption(ENUMERATE_KEYS_OPTION);
        // Unused for now, we could use a child classloader or something but for now needs to be in classpath.
        String cassandraYaml = cmd.getOptionValue(CONFIG_YAML_OPTION);
    }
}
