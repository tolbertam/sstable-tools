package com.csforge.sstable;

import ch.qos.logback.classic.LoggerContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Driver {
    public static void main(String ... args) {
        Logger.getRootLogger().setLevel(Level.OFF);
        ((LoggerContext) LoggerFactory.getILoggerFactory()).stop();
        if(args.length == 0) {
            System.err.println("Provide tool to run from ['toJson']");
            System.exit(1);
        }
        switch(args[0]) {
            case "toJson":
                SSTable2Json.main(Arrays.copyOfRange(args, 1, args.length));
                break;
            default:
                System.err.println("Unknown Command");
                System.exit(1);
        }
    }
}
