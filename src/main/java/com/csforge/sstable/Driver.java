package com.csforge.sstable;

import com.google.common.base.Strings;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.hints.HintsTool;

import java.io.File;
import java.util.Arrays;

public class Driver {

    static {
        Config.setClientMode(true);
    }

    public static void main(String ... args) {
        if (args.length == 0) {
            printCommands();
            System.exit(-1);
        }
        switch(args[0].toLowerCase()) {
            case "cqlsh":
                Cqlsh.main(Arrays.copyOfRange(args, 1, args.length));
                break;

            case "describe":
                String path = new File(args[1]).getAbsolutePath();
                try {
                    System.out.println("\u001B[1;34m" + path);
                    System.out.println(TableTransformer.ANSI_CYAN + Strings.repeat("=", path.length()));
                    System.out.print(TableTransformer.ANSI_RESET);
                    CassandraUtils.printStats(path, System.out);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;

            case "hints":
                HintsTool.main(Arrays.copyOfRange(args, 1, args.length));
                break;

            case "compact":
                Compact.main(Arrays.copyOfRange(args, 1, args.length));
                break;

            default:
                System.err.println("Unknown command: " + args[0]);
                printCommands();
                System.exit(-2);
                break;
        }
    }

    private static void printCommands() {
        System.err.println("Available commands: cqlsh, describe, hints, commitlog");
    }
}
