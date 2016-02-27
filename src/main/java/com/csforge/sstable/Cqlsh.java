package com.csforge.sstable;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.FileNameCompleter;
import jline.console.history.FileHistory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * This is just an early hacking - proof of concept (don't judge)
 *
 * - TODO : REWRITE EVERYTHING FROM SCRATCH
 *
 * - TODO : UDTs, and UDFs
 * - TODO : EXPAND ON; like cqlsh for wide rows/tiny consoles
 * - TODO : File completer on sstable use/select
 */
public class Cqlsh {

    private static final Options options = new Options();

    private static final String SCHEMA_OPTION = "s";

    private static final String FILE_OPTION = "f";
    private static final String MISSING_SSTABLES = TableTransformer.ANSI_RED +
            "No sstables set. Set the sstables using the 'USE pathToSSTable' command." +
            TableTransformer.ANSI_RESET;

    static {
        Option schemaOption = new Option(SCHEMA_OPTION, true, "Schema file to use.");
        schemaOption.setRequired(false);
        Option fileOption = new Option(FILE_OPTION, true, "Execute commands from FILE, then exit.");
        options.addOption(schemaOption);
        options.addOption(fileOption);
    }

    public List<File> sstables = Lists.newArrayList();
    public FileHistory history = null;
    private final String prompt = "\u001B[1;33mcqlsh\u001B[33m> \u001B[0m";
    public CFMetaData metadata = null;
    private boolean done = false;
    ConsoleReader console;

    String innerBuffer;
    boolean inner = false;

    public Cqlsh() {
        try {
            history = new FileHistory(new File(System.getProperty("user.home"), ".sstable-tools-cqlsh"));
            console = new ConsoleReader();
            console.setPrompt(prompt);
            console.setHistory(history);
            console.setHistoryEnabled(true);
            console.addCompleter(new FileNameCompleter());
            console.setHandleUserInterrupt(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startShell() throws Exception {
        try {
            String line = null;
            while (!done && (line = console.readLine()) != null) {
                evalLine(line);
            }
            if (line == null) {
                done = true;
            }
        } catch (IOException e) {
            System.err.println("Error in console session: " + e.getMessage());
            System.exit(-4);
        }
    }

    public void doUse(String command) throws Exception {
        String rest = command.substring(4); // "USE "
        Pattern p = Pattern.compile("((\"[^\"]+\")|[^\" ]+)");
        Matcher m = p.matcher(rest);
        this.sstables = Lists.newArrayList();

        while (m.find()) {
            String arg = m.group().replace("\"", "");
            File sstable = new File(arg);
            if (sstable.exists() && sstable.isFile()) {
                this.sstables.add(sstable);
            } else {
                try {
                    if (sstable.exists()) {
                        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**/*-Data.db");
                        Files.walkFileTree(Paths.get(arg), Sets.newHashSet(), 1, new SimpleFileVisitor<Path>() {
                            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                                if (matcher.matches(path)) {
                                    sstables.add(path.toFile());
                                }
                                return FileVisitResult.CONTINUE;
                            }

                            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                                return FileVisitResult.CONTINUE;
                            }
                        });
                    } else {
                        System.out.println("Cannot find " + sstable.getAbsolutePath());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        for (File f : sstables) {
            System.out.println("Using: " + f.getAbsolutePath());
        }
        if (!sstables.isEmpty()) {
            metadata = CassandraUtils.tableFromBestSource(sstables.get(0));
        }
    }

    public void doDump(String command) throws Exception {
        if(sstables.isEmpty()) {
            System.out.println(MISSING_SSTABLES);
            return;
        }
        Query query;
        if (command.length() > 5) {
            query = getQuery("select * from sstables " + command.substring(5));
        } else {
            query = getQuery("select * from sstables");
        }
        Stream<UnfilteredRowIterator> partitions = CassandraUtils.asStream(query.getScanner());

        partitions.forEach(partition -> {
            if(!partition.partitionLevelDeletion().isLive()) {
                System.out.println("[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "] " +
                        partition.partitionLevelDeletion());
            }
            if (!partition.staticRow().isEmpty()) {
                System.out.println(
                    "[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "] " +
                            partition.staticRow().toString(metadata, true));
            }
            partition.forEachRemaining(row -> {
                System.out.println(
                        "[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "] " +
                                row.toString(metadata, true));
            });
        });
        System.out.println();
    }

    public void doCreate(String command) throws Exception {
        innerBuffer = command;
        inner = true;
        history.removeLast();
        console.setHistoryEnabled(false);
        console.setPrompt("...    ");
        try {
            while (inner) {
                try {
                    ParsedStatement statement = QueryProcessor.parseStatement(innerBuffer);
                    inner = false;
                    history.add(innerBuffer);
                    if (statement instanceof CreateTableStatement.RawStatement) {
                        CassandraUtils.cqlOverride = innerBuffer;
                    } else if (statement instanceof CreateTypeStatement) {
                        try {
                            // TODO work around type mess
                            System.out.println(CassandraUtils.callPrivate(statement, "createType"));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    //CreateTableStatement$RawStatement
                } catch (SyntaxException e) {
                    if (!innerBuffer.trim().endsWith(";")) {
                        String line = console.readLine();
                        if (!inner) {
                            evalLine(line);
                        } else {
                            innerBuffer += " " + line;
                        }
                    } else {
                        inner = false;
                        System.out.println(TableTransformer.ANSI_RED + e.getMessage() + TableTransformer.ANSI_RESET);
                    }
                }
            }
        } finally {
            console.setHistoryEnabled(true);
            console.setPrompt(prompt);
            innerBuffer = "";
            inner = false;
        }
    }

    public Query getQuery(String command) throws Exception {
        SelectStatement.RawStatement statement = (SelectStatement.RawStatement) QueryProcessor.parseStatement(command);
        Query query;
        if (statement.columnFamily().matches("sstables?")) {
            if(sstables.isEmpty()) {
                return null;
            }
            metadata = CassandraUtils.tableFromBestSource(sstables.get(0));
            return new Query(command, sstables, metadata);
        } else {
            File path = new File(statement.columnFamily());
            if (!path.exists()) {
                throw new FileNotFoundException(path.getAbsolutePath());
            }
            metadata = CassandraUtils.tableFromBestSource(path);
            return new Query(command, Collections.singleton(path), metadata);
        }
    }

    public void evalLine(String line) throws Exception {
        // TODO: Handle semi-colons in quoted identifers.
        String cmds[] = line.split(";");
        for (String command : cmds) {
            command = command.trim();
            if (command.equals("exit") || command.equals("quit")) {
                done = true;
                continue;
            } else if (command.toLowerCase().trim().startsWith("describe schema")) {
                if(CassandraUtils.cqlOverride != null) {
                    System.out.println(CassandraUtils.cqlOverride);
                } else if (metadata != null) {
                    System.out.println(metadata);
                } else {
                    System.err.format("%sNo current metadata set, use a CREATE TABLE statement to set%s%n",
                            TableTransformer.ANSI_RED, TableTransformer.ANSI_RESET);
                }
                continue;
            } else if (command.toLowerCase().trim().startsWith("describe sstable")) {
                System.out.println();
                for(File f : sstables) {
                    System.out.println("\u001B[1;34m" + f.getAbsolutePath());
                    System.out.println(TableTransformer.ANSI_CYAN +
                            Strings.repeat("=", f.getAbsolutePath().length()) +
                            TableTransformer.ANSI_RESET);
                    CassandraUtils.printStats(f.getAbsolutePath(), System.out, true);
                    System.out.println();
                }
                continue;
            } else if (command.toLowerCase().startsWith("use ")) {
                doUse(command);
                continue;
            } else if (command.toLowerCase().startsWith("dump")) {
                doDump(command);
                continue;
            } else if (command.toLowerCase().equals("help")) {
                try {
                    System.out.println(Resources.toString(Resources.getResource("cqlsh-help"), Charsets.UTF_8));
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-5);
                }
                continue;
            } else if (command.length() >= 6) {
                String queryType = command.substring(0, 6).toLowerCase();
                switch (queryType) {
                    case "select":
                        Query q = getQuery(command);
                        if(q == null) {
                            System.out.println(MISSING_SSTABLES);
                        } else {
                            TableTransformer.dumpResults(metadata, getQuery(command).getResults(), System.out);
                        }
                        continue;
                    case "create":
                        doCreate(command);
                        continue;
                    case "update":
                    case "insert":
                    case "delete":
                        System.err.format("%sQuery '%s' is not supported since this tool is read-only.%s%n",
                                TableTransformer.ANSI_RED, command, TableTransformer.ANSI_RESET);
                        continue;
                }
            }
            System.err.format("%sUnknown command: %s%s%n", TableTransformer.ANSI_RED, command,
                    TableTransformer.ANSI_RESET);
        }
    }

    public static void main(String args[]) {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.format("%sFailure parsing arguments: %s%s%n%n", TableTransformer.ANSI_RED, e.getMessage(),
                    TableTransformer.ANSI_RESET);
            try (PrintWriter errWriter = new PrintWriter(System.err, true)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(errWriter, 120, "cqlsh sstable [sstable ...]",
                        String.format("%nOffline CQL Shell for Apache Cassandra 3.x%nOptions:"),
                        options, 2, 1, "", true);
            } finally {
                System.exit(-1);
            }
        }

        final Cqlsh sh = new Cqlsh();

        String schemaPath = cmd.getOptionValue(SCHEMA_OPTION);
        if (schemaPath != null) {
            try (InputStream schemaStream = new FileInputStream(new File(schemaPath))) {
                sh.metadata = CassandraUtils.tableFromCQL(schemaStream);
            } catch (IOException e) {
                System.err.println("Error reading schema metadata: " + e.getMessage());
                System.exit(-2);
            }
            System.setProperty("sstabletools.schema", schemaPath);
        }

        List<File> sstables = Lists.newArrayList();
        for (String sstable : cmd.getArgs()) {
            File file = new File(sstable);
            if (!file.exists()) {
                System.err.println("Non-existant sstable file provided: " + sstable);
                System.exit(-3);
            }
            sstables.add(file);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    sh.history.flush();
                } catch (IOException e) {
                    System.err.println("Couldn't flush history on shutdown.  Reason:" + e.getMessage());
                }
            }
        });

        String cqlFilePath = cmd.getOptionValue(FILE_OPTION);
        if (cqlFilePath != null) {
            try (Scanner s = new Scanner(new FileInputStream(cqlFilePath))) {
                while (s.hasNextLine()) {
                    try {
                        sh.evalLine(s.nextLine());
                    } catch (Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            } catch (FileNotFoundException ex) {
                System.err.println("Cannot find " + cqlFilePath);
                System.exit(-4);
            }
        } else {

            while (!sh.done) {
                try {
                    sh.startShell();
                } catch (UserInterruptException e) {
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                sh.history.flush();
            } catch (IOException e) {
            }
            sh.console.shutdown();
        }

    }
}
