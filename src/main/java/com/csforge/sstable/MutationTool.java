package com.csforge.sstable;

import com.datastax.driver.core.Cluster;
import com.google.common.base.Strings;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.commons.cli.*;
import org.mockito.internal.util.io.IOUtil;

import java.io.File;
import java.io.PrintStream;

import static com.csforge.sstable.TableTransformer.*;

public abstract class MutationTool {
    private static final String HOST_OPTION = "h";
    private static final String PORT_OPTION = "p";
    private static final String CFID_OPTION = "i";
    private static final String REPLAY_OPTION = "r";
    private static final String SILENT_OPTION = "s";
    protected static final Options options = new Options();

    static {
        DatabaseDescriptor.clientInitialization(false);
        Option hostOption = new Option(HOST_OPTION, true, "Host to extract schema from.");
        hostOption.setRequired(false);
        options.addOption(hostOption);
        Option portOption = new Option(PORT_OPTION, true, "CQL native port.");
        portOption.setRequired(false);
        options.addOption(portOption);
        Option cfidOption = new Option(CFID_OPTION, true, "Override cfids by table name.");
        cfidOption.setRequired(false);
        options.addOption(cfidOption);
        Option silentOption = new Option(SILENT_OPTION, false, "Only output mutations.");
        silentOption.setRequired(false);
        options.addOption(silentOption);
        Option replayOption = new Option(REPLAY_OPTION, false, "Replay the mutations on cluster the node set in -h belongs to.");
        replayOption.setRequired(false);
        options.addOption(replayOption);
    }

    protected abstract void walkMutations(File target, PrintStream out, MutationReplayer mutationReplayer) throws Exception;

    protected abstract void printHelp();

    protected CommandLineParser parser = new PosixParser();
    protected CommandLine cmd = null;
    protected String host;
    protected int port;
    protected Cluster cluster;

    public MutationTool(String... args) {
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.format("%sFailure parsing arguments: %s%s%n%n", ANSI_RED, e.getMessage(), ANSI_RESET);
            printHelp();
            System.exit(-1);

        }
        if (cmd.getArgs().length == 0) {
            printHelp();
            System.exit(-2);
        }
        host = cmd.getOptionValue(HOST_OPTION, "127.0.0.1");
        port = Integer.parseInt(cmd.getOptionValue(PORT_OPTION, "9042"));
    }

    public void run() {
        try {
            try {
                if (!cmd.hasOption(SILENT_OPTION)) {
                    System.out.printf("%sLoading schema from %s:%s%s", ANSI_RED, host, port, ANSI_RESET);
                    System.out.flush();
                }
                String cfids = cmd.getOptionValue(CFID_OPTION, "");
                cluster = CassandraUtils.loadTablesFromRemote(host, port, cfids);
            } finally {
                System.out.print(ANSI_RESET);
            }
                for (String target : cmd.getArgs()) {
                    File targetFile = new File(target);
                    if (!targetFile.exists()) {
                        if (!cmd.hasOption(SILENT_OPTION)) {
                            System.out.print("\r" + ANSI_RESET + Strings.repeat(" ", 60) + "\r");
                        }
                        System.err.println("Non-existant target provided: " + target);
                    } else {
                        if (!cmd.hasOption(SILENT_OPTION)) {
                            System.out.print("\r" + ANSI_RESET + Strings.repeat(" ", 60) + "\r");
                            System.out.println("\u001B[1;34m" + targetFile.getAbsolutePath());
                            System.out.println(ANSI_CYAN + Strings.repeat("=", targetFile.getAbsolutePath().length()));
                            System.out.print(ANSI_RESET);
                        }
                        MutationReplayer replayer = null;
                        if (cmd.hasOption(REPLAY_OPTION)) {
                            replayer = new MutationReplayer(cluster);
                        }
                        walkMutations(targetFile, System.out, replayer);
                    }
                }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtil.closeQuietly(cluster);
        }
    }
}
