package com.csforge.sstable;

import com.csforge.reader.CassandraReader;
import com.csforge.reader.Partition;
import com.csforge.sstable.json.JsonTransformer;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;

public class SSTable2Json {

    private static final Options options = new Options();

    private static final String PARTITION_KEY_OPTION = "k";

    private static final String EXCLUDE_KEY_OPTION = "x";

    private static final String CREATE_OPTION = "c";

    private static final String ENUMERATE_KEYS_OPTION = "e";

    static {
        Config.setClientMode(true);

        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        Option partitionKey = new Option(PARTITION_KEY_OPTION, true, "Partition Keys to be included");
        partitionKey.setArgs(Option.UNLIMITED_VALUES);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, true, "Excluded Partition Key");
        excludeKey.setArgs(Option.UNLIMITED_VALUES);

        Option enumerateKeys = new Option(ENUMERATE_KEYS_OPTION, false, "Enumerate keys only");

        Option cqlCreate = new Option(CREATE_OPTION, true, "file containing \"CREATE TABLE...\" for the sstables schema");

        options.addOption(partitionKey);
        options.addOption(excludeKey);
        options.addOption(enumerateKeys);
        options.addOption(cqlCreate);
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
        HashSet<String> excludes =  new HashSet<String>(Arrays.asList(cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null?
                new String[0] : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));
        boolean enumerateKeysOnly = cmd.hasOption(ENUMERATE_KEYS_OPTION);
        String create = cmd.getOptionValue(CREATE_OPTION);

        try {
            String cql = new String(Files.readAllBytes(Paths.get(create)));
            CassandraReader reader = new CassandraReader(cql);

            Stream<Partition> partitions = keys == null?
                    reader.readSSTable(sstablePath, excludes) :
                    reader.readSSTable(sstablePath, Arrays.stream(keys), excludes);
            JsonTransformer.toJson(partitions, reader.getMetadata(), System.out);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
