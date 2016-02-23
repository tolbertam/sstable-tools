package com.csforge.sstable;

import com.csforge.sstable.reader.CassandraReader;
import com.csforge.sstable.reader.Partition;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Stream;

public class SSTable2Json {

    private static final Logger logger = LoggerFactory.getLogger(SSTable2Json.class);

    private static final Options options = new Options();

    private static final String PARTITION_KEY_OPTION = "k";

    private static final String EXCLUDE_KEY_OPTION = "x";

    private static final String COMPACT_KEY_OPTION = "c";

    private static final String ENUMERATE_KEYS_OPTION = "e";

    static {
        Option partitionKey = new Option(PARTITION_KEY_OPTION, true, "Partition key to be included.  May be used multiple times.  If not set will default to all keys.");
        partitionKey.setArgs(Option.UNLIMITED_VALUES);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "Partition key to be excluded.  May be used multiple times.");
        excludeKey.setArgs(Option.UNLIMITED_VALUES);

        Option enumerateKeys = new Option(ENUMERATE_KEYS_OPTION, false, "Only print out the keys for the sstable.  If enabled other options are ignored.");

        Option compact = new Option(COMPACT_KEY_OPTION, false, "Print one partition per line.");

        options.addOption(partitionKey);
        options.addOption(excludeKey);
        options.addOption(enumerateKeys);
        options.addOption(compact);
    }

    public static void main(String args[]) {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            if (args.length == 0) {
                throw new ParseException("You must supply exactly one sstable.");
            }
            cmd = parser.parse(options, args);
            if (cmd.getArgs().length != 1) {
                logger.warn("Multiple SSTables provided, only using {}.", cmd.getArgs()[0]);
            }
        } catch (ParseException e) {
            System.err.println("Failure parsing arguments: " + e.getMessage());
            try (PrintWriter errWriter = new PrintWriter(System.err, true)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(errWriter, 120, "toJson <sstable>",
                        System.getProperty("line.separator") + "Converts SSTable into a JSON formatted document.",
                        options, 2, 1, "", true);
            } finally {
                System.exit(-1);
            }
        }

        File sstablePath = new File(cmd.getArgs()[0]);
        String[] keys = cmd.getOptionValues(PARTITION_KEY_OPTION);
        HashSet<String> excludes = new HashSet<String>(Arrays.asList(cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null ?
                new String[0] : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));
        boolean enumerateKeysOnly = cmd.hasOption(ENUMERATE_KEYS_OPTION);
        boolean compact = cmd.hasOption(COMPACT_KEY_OPTION);
        try {
            CFMetaData metadata = CassandraUtils.tableFromBestSource(sstablePath);
            CassandraReader reader = new CassandraReader(metadata);

            if (enumerateKeysOnly) {
                Stream<DecoratedKey> sstableKeys = reader.keys(sstablePath);
                JsonTransformer.keysToJson(sstableKeys, reader.getMetadata(), compact, System.out);
            } else {
                Stream<Partition> partitions = keys == null ?
                        reader.readSSTable(sstablePath, excludes) :
                        reader.readSSTable(sstablePath, Arrays.stream(keys), excludes);
                JsonTransformer.toJson(partitions, reader.getMetadata(), compact, System.out);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println();
    }
}
