package org.apache.cassandra.hints;


import com.csforge.sstable.MutationReplayer;
import com.csforge.sstable.MutationTool;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.commons.cli.HelpFormatter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class HintsTool extends MutationTool {

    public HintsTool(String... args) {
        super(args);
    }

    protected void walkMutations(File target, PrintStream out, MutationReplayer replayer) throws Exception {
        int version = Integer.parseInt(System.getProperty("hints.version", "" + MessagingService.current_version));
        try (HintsReader reader = HintsReader.open(target)) {
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
                    if (replayer != null) {
                        replayer.sendMutation(mutation);
                    }
                }
            }
        }
    }

    protected void printHelp() {
        try (PrintWriter errWriter = new PrintWriter(System.err, true)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(errWriter, 120, "hints hintfile [hintfile ...]",
                    String.format("%nHint Dump for Apache Cassandra 3.x%nOptions:"), options, 2, 1, "", true);
            errWriter.println("  hintfile at least one file containing hints");
            errWriter.println();
        }
    }

    public static void main(String... args) {
        new HintsTool(args).run();
    }
}
