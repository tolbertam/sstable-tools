package org.apache.cassandra.hints;


import com.csforge.sstable.CassandraUtils;
import com.google.common.base.Strings;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.csforge.sstable.TableTransformer.*;

public class HintsTool {
    private static final String HOST_OPTION = "h";
    private static final String PORT_OPTION = "p";
    private static final String SILENT_OPTION = "s";
    private static final Options options = new Options();

    static {
        Config.setClientMode(true);
        Option hostOption = new Option(HOST_OPTION, true, "Host to extract schema frome.");
        hostOption.setRequired(false);
        options.addOption(hostOption);
        Option portOption = new Option(PORT_OPTION, true, "CQL native port.");
        portOption.setRequired(false);
        options.addOption(portOption);
        Option silentOption = new Option(SILENT_OPTION, false, "Only output mutations.");
        silentOption.setRequired(false);
        options.addOption(silentOption);
    }

    private static void dumpHints(File hint_file, PrintStream out, int version) throws IOException {
        try (HintsReader reader = HintsReader.open(hint_file)) {
            for (HintsReader.Page page : reader) {
                Iterator<ByteBuffer> hints = page.buffersIterator(); //hints iterator im pretty sure is busted
                while (hints.hasNext()) {
                    ByteBuffer buf = hints.next();
                    DataInputPlus in = new DataInputPlus.DataInputStreamPlus(new ByteArrayInputStream(buf.array()));
                    Hint hint = Hint.serializer.deserialize(in, version);
                    Mutation mutation = hint.mutation;
                    for (PartitionUpdate partition : mutation.getPartitionUpdates()) {
                        out.println(partition);
                        for (Row row : partition) {
                            out.println(row);
                        }
                    }
                }
            }
        }
    }

    private static void printHelp() {
        try (PrintWriter errWriter = new PrintWriter(System.err, true)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(errWriter, 120, "hints hintfile [hintfile ...]",
                    String.format("%nHint Dump for Apache Cassandra 3.x%nOptions:"),
                    options, 2, 1, "", true);
            errWriter.println("  hintfile at least one file containing hints");
            errWriter.println();
        }
    }

    public static void main(String... args) {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.format("%sFailure parsing arguments: %s%s%n%n", ANSI_RED, e.getMessage(), ANSI_RESET);
            printHelp();
            System.exit(-1);

        }
        if(cmd.getArgs().length == 0) {
            printHelp();
            System.exit(-2);
        }
        String host = cmd.getOptionValue(HOST_OPTION, "127.0.0.1");
        int port = Integer.parseInt(cmd.getOptionValue(PORT_OPTION, "9042"));
        try {
            int version = MessagingService.current_version;
            try {
                if (!cmd.hasOption(SILENT_OPTION)) {
                    System.out.printf("%sLoading schema from %s:%s%s", ANSI_RED, host, port, ANSI_RESET);
                    System.out.flush();
                }
                CassandraUtils.loadTablesFromRemote(host, port);
                version = Integer.parseInt(System.getProperty("hints.version", "" + MessagingService.current_version));
            } finally {
                System.out.print(ANSI_RESET);
            }
            for (String hint : cmd.getArgs()) {
                File hintFile = new File(hint);
                if (!hintFile.exists()) {
                    if(!cmd.hasOption(SILENT_OPTION)) {
                        System.out.println("\r" + ANSI_RESET);
                    }
                    System.err.println("Non-existant hint file provided: " + hint);
                } else {
                    if(!cmd.hasOption(SILENT_OPTION)) {
                        System.out.println("\r\u001B[1;34m" + hintFile.getAbsolutePath());
                        System.out.println(ANSI_CYAN + Strings.repeat("=", hintFile.getAbsolutePath().length()));
                        System.out.print(ANSI_RESET);
                    }
                    dumpHints(hintFile, System.out, version);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
