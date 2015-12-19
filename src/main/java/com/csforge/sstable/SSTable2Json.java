package com.csforge.sstable;

import com.csforge.sstable.reader.CassandraReader;
import com.csforge.sstable.reader.Partition;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Stream;

public class SSTable2Json {

    private static final Logger logger = LoggerFactory.getLogger(SSTable2Json.class);

    private static final Options options = new Options();

    private static final String PARTITION_KEY_OPTION = "k";

    private static final String EXCLUDE_KEY_OPTION = "x";

    private static final String CREATE_OPTION = "c";

    private static final String ENUMERATE_KEYS_OPTION = "e";

    private static final String SHORT_KEYS_OPTION = "s";

    static {
        Config.setClientMode(true);

        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        Option partitionKey = new Option(PARTITION_KEY_OPTION, true, "Partition key to be included.  May be used multiple times.  If not set will default to all keys.");
        partitionKey.setArgs(Option.UNLIMITED_VALUES);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "Partition key to be excluded.  May be used multiple times.");
        excludeKey.setArgs(Option.UNLIMITED_VALUES);

        Option enumerateKeys = new Option(ENUMERATE_KEYS_OPTION, false, "Only print out the keys for the sstable.  If enabled other options are ignored.");

        Option cqlCreate = new Option(CREATE_OPTION, true, "Optional file containing \"CREATE TABLE...\" for the sstable's schema.  " +
                "Used to determine the partition and clustering key names. Must not include \"keyspace.\" in create statement.  " +
                "If not included will not print key names.");

        options.addOption(partitionKey);
        options.addOption(excludeKey);
        options.addOption(enumerateKeys);
        options.addOption(cqlCreate);
    }

    public static void main(String args[]) {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            if(args.length == 0) {
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

        String sstablePath = cmd.getArgs()[0];
        String[] keys = cmd.getOptionValues(PARTITION_KEY_OPTION);
        HashSet<String> excludes =  new HashSet<String>(Arrays.asList(cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null?
                new String[0] : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));
        boolean enumerateKeysOnly = cmd.hasOption(ENUMERATE_KEYS_OPTION);
        String create = cmd.getOptionValue(CREATE_OPTION);
        try {
            CassandraReader reader = null;
            boolean shortKeys = create == null;
            if(create != null) {
                String cql = new String(Files.readAllBytes(Paths.get(create)));
                reader = CassandraReader.fromCql(cql);
            } else {
                reader = CassandraReader.fromSSTable(sstablePath);
            }

            if(enumerateKeysOnly) {
                Stream<DecoratedKey> sstableKeys = reader.keys(sstablePath);
                JsonTransformer.keysToJson(sstableKeys, reader.getMetadata(), shortKeys, System.out);
            } else {
                Stream<Partition> partitions = keys == null ?
                        reader.readSSTable(sstablePath, excludes) :
                        reader.readSSTable(sstablePath, Arrays.stream(keys), excludes);
                JsonTransformer.toJson(partitions, reader.getMetadata(), shortKeys, System.out);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println();
    }
}
