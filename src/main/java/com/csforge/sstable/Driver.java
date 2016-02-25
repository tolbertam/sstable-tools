package com.csforge.sstable;

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

            default:
                System.err.println("Unknown command: " + args[0]);
                printCommands();
                System.exit(-2);
                break;
        }
    }

    private static void printCommands() {
        System.err.println("Available commands: cqlsh, toJson, select");
    }
}
