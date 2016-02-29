package com.csforge.sstable;

import com.google.common.base.Strings;

import java.io.File;
import java.util.Arrays;

public class Driver {
    public static void main(String ... args) {
        if (args.length == 0) {
            printCommands();
            System.exit(-1);
        }
        switch(args[0].toLowerCase()) {
            case "tojson":
                SSTable2Json.main(Arrays.copyOfRange(args, 1, args.length));
                break;

            case "select":
                Query.main(Arrays.copyOfRange(args, 0, args.length));
                break;

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

            default:
                System.err.println("Unknown command: " + args[0]);
                printCommands();
                System.exit(-2);
                break;
        }
    }

    private static void printCommands() {
        System.err.println("Available commands: cqlsh, toJson, select, describe");
    }
}
