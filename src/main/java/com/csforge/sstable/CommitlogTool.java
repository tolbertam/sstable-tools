package com.csforge.sstable;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Future;

import static com.csforge.sstable.TableTransformer.*;


/**
 * TODO: this is a giant hackery hack hack until CommitLogReader in  -- CASSANDRA-8844 --  is in a release
 *
 *  there be dragons here
 */
public class CommitlogTool {
    private static final String HOST_OPTION = "h";
    private static final String PORT_OPTION = "p";
    private static final String SILENT_OPTION = "s";
    private static final Options options = new Options();

    static {
        Config.setClientMode(true);
        Option hostOption = new Option(HOST_OPTION, true, "Host to extract schema from.");
        hostOption.setRequired(false);
        options.addOption(hostOption);
        Option portOption = new Option(PORT_OPTION, true, "CQL native port.");
        portOption.setRequired(false);
        options.addOption(portOption);
        Option silentOption = new Option(SILENT_OPTION, false, "Only output mutations.");
        silentOption.setRequired(false);
        options.addOption(silentOption);
        try {
            Config conf = (Config) CassandraUtils.readPrivate(DatabaseDescriptor.class, "conf");
            // to stop commitlog's statically initialized singleton from doing something stupid
            // we have to give some dummy stuff here to placate it
            conf.max_mutation_size_in_kb = conf.commitlog_segment_size_in_mb * 1024 * 1024 / 2;
            conf.commitlog_sync = Config.CommitLogSync.batch;
            conf.commitlog_sync_batch_window_in_ms = Double.MAX_VALUE;
            conf.commitlog_directory = System.getProperty("java.io.tmpdir");
            conf.hints_directory = System.getProperty("java.io.tmpdir");
            conf.saved_caches_directory = System.getProperty("java.io.tmpdir");
            conf.data_file_directories = new String[] {System.getProperty("java.io.tmpdir")};
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            System.exit(33);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            System.exit(32);
        }
    }

    private static void dumpCommitLog(File commitLog, PrintStream out) throws Exception {
        Class replayFilterClass = Class.forName("org.apache.cassandra.db.commitlog.CommitLogReplayer$ReplayFilter");
        // make filter
        Method create = replayFilterClass.getDeclaredMethod("create");
        create.setAccessible(true);
        Object filter = create.invoke(null);
        Class<?> c = Class.forName("org.apache.cassandra.db.commitlog.CommitLogReplayer");
        Constructor<?> constructor = c.getDeclaredConstructor(CommitLog.class, ReplayPosition.class, Map.class, replayFilterClass);
        constructor.setAccessible(true);
        CommitLogReplayer replayer = (CommitLogReplayer) constructor.newInstance(CommitLog.instance, ReplayPosition.NONE, Maps.newHashMap(), filter);
        replayer.mutationInitiator = new CommitLogReplayer.MutationInitiator() {
            protected Future<Integer> initiateMutation(final Mutation mutation, final long segmentId,
                                                       final int serializedSize, final int entryLocation, final CommitLogReplayer clr) {
                for (PartitionUpdate partition : mutation.getPartitionUpdates()) {
                    out.println(partition);
                    for (Row row : partition) {
                        out.println(row);
                    }
                }
                return Futures.immediateFuture(serializedSize);
            }
        };
        replayer.recover(commitLog, true);
    }

    private static void printHelp() {
        try (PrintWriter errWriter = new PrintWriter(System.err, true)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(errWriter, 120, "commitlog logfile [logfile ...]",
                    String.format("%nCommitlog Dump for Apache Cassandra 3.x%nOptions:"),
                    options, 2, 1, "", true);
            errWriter.println("  logfile at least one commitlog");
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
            try {
                if (!cmd.hasOption(SILENT_OPTION)) {
                    System.out.printf("%sLoading schema from %s:%s%s", ANSI_RED, host, port, ANSI_RESET);
                    System.out.flush();
                }
                CassandraUtils.loadTablesFromRemote(host, port);
            } finally {
                System.out.print(ANSI_RESET);
            }
            for (String commitlog : cmd.getArgs()) {
                File commitlogFile = new File(commitlog);
                if (!commitlogFile.exists()) {
                    if(!cmd.hasOption(SILENT_OPTION)) {
                        System.out.println("\r" + ANSI_RESET);
                    }
                    System.err.println("Non-existant commitlog provided: " + commitlog);
                } else {
                    if(!cmd.hasOption(SILENT_OPTION)) {
                        System.out.println("\r\u001B[1;34m" + commitlogFile.getAbsolutePath());
                        System.out.println(ANSI_CYAN + Strings.repeat("=", commitlogFile.getAbsolutePath().length()));
                        System.out.print(ANSI_RESET);
                    }

                }
                dumpCommitLog(commitlogFile, System.out);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
