package org.apache.cassandra.hints;


import com.csforge.sstable.CassandraUtils;
import com.csforge.sstable.TableTransformer;
import com.google.common.base.Strings;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class HintsTool {

    static {
        Config.setClientMode(true);
    }

    public static void main(String... args) {
        String host = "127.0.0.1";
        int port = 9042;
        File hint_file = null;

        if (args.length == 1) {
            hint_file = new File(args[0]);
        } else if (args.length == 2) {
            host = args[0];
            hint_file = new File(args[1]);
        } else if (args.length == 3) {
            host = args[0];
            port = Integer.parseInt(args[1]);
            hint_file = new File(args[2]);
        } else {
            System.err.println("Usage: hints <host> <port> <hint_file>\n       hints <host> <hint_file>\n" +
                    "       hints <hint_file>");
            System.exit(-1);
        }

        try {
            System.out.printf("%sLoading schema from %s:%s", TableTransformer.ANSI_RED, host, port);
            System.out.flush();
            CassandraUtils.loadTablesFromRemote(host, port);
            System.out.println("\r\u001B[1;34m" + hint_file.getAbsolutePath());
            System.out.println(TableTransformer.ANSI_CYAN + Strings.repeat("=", hint_file.getAbsolutePath().length()));
            System.out.print(TableTransformer.ANSI_RESET);
            int version = Integer.parseInt(System.getProperty("hints.version", "" + MessagingService.current_version));

            try (HintsReader reader = HintsReader.open(hint_file)) {
                for (HintsReader.Page page : reader) {
                    Iterator<ByteBuffer> hints = page.buffersIterator(); //hints iterator im pretty sure is busted
                    while (hints.hasNext()) {
                        ByteBuffer buf = hints.next();
                        DataInputPlus in = new DataInputPlus.DataInputStreamPlus(new ByteArrayInputStream(buf.array()));
                        Hint hint = Hint.serializer.deserialize(in, version);
                        Mutation mutation = hint.mutation;
                        for (PartitionUpdate partition : mutation.getPartitionUpdates()) {
                            System.out.println(partition);
                            for (Row row : partition) {
                                System.out.println(row);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
