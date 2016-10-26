package com.csforge.sstable;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.commons.cli.HelpFormatter;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Future;


/**
 * TODO: this is a giant hackery hack hack until CommitLogReader in  -- CASSANDRA-8844 --  is in a release
 *
 *  there be dragons here
 */
public class CommitlogTool extends MutationTool {

    static {
        // to stop commitlog's statically initialized singleton from doing something stupid
        // we have to give some dummy stuff here to placate it
        try {
            Config conf = (Config) CassandraUtils.readPrivate(DatabaseDescriptor.class, "conf");
            conf.max_mutation_size_in_kb = conf.commitlog_segment_size_in_mb * 1024 * 1024 / 2;
            conf.commitlog_sync = Config.CommitLogSync.batch;
            conf.commitlog_total_space_in_mb = 32;
            conf.commitlog_sync_batch_window_in_ms = Double.MAX_VALUE;
            conf.commitlog_directory = System.getProperty("java.io.tmpdir");
            conf.hints_directory = System.getProperty("java.io.tmpdir");
            conf.saved_caches_directory = System.getProperty("java.io.tmpdir");
            conf.data_file_directories = new String[] {System.getProperty("java.io.tmpdir")};
            DatabaseDescriptor.setEncryptionContext(new EncryptionContext(new TransparentDataEncryptionOptions()));
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            System.exit(33);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            System.exit(32);
        }
    }

    public CommitlogTool(String... args) {
        super(args);
    }

    protected void walkMutations(File target, PrintStream out, MutationReplayer mutationReplayer) throws Exception {
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
                if(mutationReplayer != null) {
                    mutationReplayer.sendMutation(mutation);
                }
                return Futures.immediateFuture(serializedSize);
            }
        };
        replayer.recover(target, true);
        CommitLog.instance.stopUnsafe(false);

    }

    protected void printHelp() {
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
        new CommitlogTool(args).run();
    }
}
