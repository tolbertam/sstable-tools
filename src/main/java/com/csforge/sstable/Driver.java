package com.csforge.sstable;

import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Driver {
    public static void main(String ... args) {
        ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
        if (args.length == 0) {
            printCommands();
            System.exit(-1);
        }
        switch(args[0]) {
            case "toJson":
                SSTable2Json.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            default:
                System.err.println("Unknown command: " + args[0]);
                printCommands();
                System.exit(-2);
                break;
        }
    }

    private static void printCommands() {
        System.err.println("Available commands: toJson");
    }
}
