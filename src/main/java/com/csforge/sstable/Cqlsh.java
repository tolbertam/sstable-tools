package com.csforge.sstable;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.*;
import jline.console.history.FileHistory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.csforge.sstable.TableTransformer.*;

/**
 * This is just an early hacking - proof of concept (don't judge)
 * <p/>
 * - TODO : REWRITE EVERYTHING FROM SCRATCH
 * <p/>
 * - TODO : UDTs, and UDFs
 * - TODO : EXPAND ON; like cqlsh for wide rows/tiny consoles
 * - TODO : File completer on sstable use/select
 */
public class Cqlsh {

    private static final Options options = new Options();

    private static final String SCHEMA_OPTION = "s";

    private static final String FILE_OPTION = "f";

    private static final String MISSING_SSTABLES = errorMsg("No sstables set. Set the sstables using the 'USE pathToSSTable' command.");

    private static final String QUERY_PAGING_ALREADY_ENABLED = errorMsg("Query paging is already enabled. Use PAGING OFF to disable.");

    private static final String QUERY_PAGING_ALREADY_DISABLED = errorMsg("Query paging is not enabled.");

    private static final String IMPROPER_PAGING_COMMAND = errorMsg("Improper PAGING command.");

    private static final String QUERY_PAGING_ENABLED = "Now Query paging is enabled%nPage size: %d%n";

    private static final String QUERY_PAGING_DISABLED = "Disabled Query paging.";

    private static final String PAGING_IS_ENABLED = "Query paging is currently enabled. Use PAGING OFF to disable%nPage size: %d%n";

    private static final String PAGING_IS_DISABLED = "Query paging is currently disabled. Use PAGING ON to enable.";

    private static final String CANNOT_FIND_FILE = errorMsg("Cannot find '%s'.%n");

    private static final String IMPORTED_SCHEMA = "Successfully imported schema from '%s'.%n";

    private static final String FAILED_TO_IMPORT_SCHEMA = errorMsg("Could not import schema from '%s': %s.%n");

    private static final String PERSISTENCE_NOTE = infoMsg("To not persist settings, use PERSIST OFF.");

    private static final String PREFERENCES_ARE_DISABLED = "Preferences are currently disabled.";

    private static final String PREFERENCES_ARE_ENABLED = "Preferences are currently enabled:%n%s";

    private static final String PREFERENCES_ALREADY_ENABLED = errorMsg("Preferences are already enabled.  Using PERSIST OFF to disable.");

    private static final String PREFERENCES_ALREADY_DISABLED = errorMsg("Preferences are not enabled.");

    private static final String PREFERENCES_ENABLED = "Now Preferences are enabled:%n%s";

    private static final String PREFERENCES_DISABLED = "Disabled Preferences.";

    private static final String DISABLING_SCHEMA = "Disabling user-defined schema and using sstable metadata instead.";

    private static final String USER_DEFINED_SCHEMA = "User-defined schema is:%n%s%n";

    private static final String NO_USER_DEFINED_SCHEMA = "No user-defined schema, using sstable metadata instead.";

    private static final File CONFIG_DIR = new File(System.getProperty("user.home"), ".sstable-tools");

    private static final File HISTORY_FILE = new File(CONFIG_DIR, ".history");

    private static final File PREFERENCES_FILE = new File(CONFIG_DIR, ".preferences.conf");

    private static final String PROP_SSTABLES = "sstables";

    private static final String PROP_SCHEMA = "schema";

    private static final String PROP_PAGING_ENABLED = "pagingEnabled";

    private static final String PROP_PAGING_SIZE = "pagingSize";

    private static final String PROP_PREFERENCES_ENABLED = "preferencesEnabled";

    static {
        if (!CONFIG_DIR.exists()) {
            boolean created = CONFIG_DIR.mkdirs();
            if (!created) {
                System.err.println("Failed to create preferences directory: " + CONFIG_DIR);
            }
        }
    }

    static String errorMsg(String msg) {
        return ANSI_RED + msg + ANSI_RESET;
    }

    static String infoMsg(String msg) {
        return ANSI_CYAN + msg + ANSI_RESET;
    }

    static {
        Option schemaOption = new Option(SCHEMA_OPTION, true, "Schema file to use.");
        schemaOption.setRequired(false);
        Option fileOption = new Option(FILE_OPTION, true, "Execute commands from FILE, then exit.");
        options.addOption(schemaOption);
        options.addOption(fileOption);
    }

    public Set<File> sstables = Sets.newHashSet();
    public FileHistory history = null;
    private final String prompt = "\u001B[1;33mcqlsh\u001B[33m> \u001B[0m";
    public CFMetaData metadata = null;
    private boolean done = false;
    ConsoleReader console;

    String innerBuffer;
    boolean inner = false;
    boolean paging = true;
    int pageSize = 100;
    boolean preferences = true;
    Config config;

    public Cqlsh() {
        try {
            Config applicationConfig = ConfigFactory.defaultApplication();
            config = ConfigFactory.parseFile(PREFERENCES_FILE).withFallback(applicationConfig);
            paging = config.getBoolean(PROP_PAGING_ENABLED);
            pageSize = config.getInt(PROP_PAGING_SIZE);
            sstables = config.getStringList(PROP_SSTABLES).stream().map(File::new).filter(f -> {
                if (!f.exists()) {
                    System.err.printf(CANNOT_FIND_FILE, f.getAbsolutePath());
                }
                return f.exists();
            }).collect(Collectors.toSet());

            boolean persistInfo = false;
            if (sstables.size() > 0) {
                System.out.println(infoMsg("Using previously defined sstables: " + sstables));
                persistInfo = true;
            }

            preferences = config.getBoolean(PROP_PREFERENCES_ENABLED);
            String schema = config.getString(PROP_SCHEMA);
            if (!Strings.isNullOrEmpty(schema)) {
                System.out.printf(infoMsg("Using previously defined schema:%n%s%n"), schema);
                persistInfo = true;
                CassandraUtils.cqlOverride = schema;
            }

            if (persistInfo) {
                System.out.println(PERSISTENCE_NOTE);
            }

            history = new FileHistory(HISTORY_FILE);
            console = new ConsoleReader();
            console.setPrompt(prompt);
            console.setHistory(history);
            console.setHistoryEnabled(true);
            List<Completer> completers = Lists.newArrayList();

            ArgumentCompleter argCompleter = new ArgumentCompleter(
                    caselessCompleter("use"),
                    new FileNameCompleter()
            );
            completers.add(argCompleter);
            argCompleter = new ArgumentCompleter(
                    caselessCompleter("describe"),
                    caselessCompleter("sstables", "schema")
            );
            completers.add(argCompleter);
            argCompleter = new ArgumentCompleter(
                    caselessCompleter("dump"),
                    caselessCompleter("where")
            );
            completers.add(argCompleter);
            argCompleter = new ArgumentCompleter(
                    caselessCompleter("create"),
                    caselessCompleter("table")
            );
            completers.add(argCompleter);
            argCompleter = new ArgumentCompleter(
                    caselessCompleter("paging"),
                    caselessCompleter("on", "off")
            );
            completers.add(argCompleter);
            argCompleter = new ArgumentCompleter(
                    caselessCompleter("persist"),
                    caselessCompleter("on", "off")
            );
            completers.add(argCompleter);
            argCompleter = new ArgumentCompleter(
                    caselessCompleter("schema"),
                    new AggregateCompleter(new FileNameCompleter(), caselessCompleter("on", "off"))
            );
            completers.add(argCompleter);
            completers.add(caselessCompleter("exit", "help", "select"));
            for (Completer c : completers) {
                console.addCompleter(c);
            }
            console.setHandleUserInterrupt(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static AggregateCompleter caselessCompleter(String... args) {
        return new AggregateCompleter(
                new StringsCompleter(Arrays.stream(args).map(String::toUpperCase).collect(Collectors.toList())),
                new StringsCompleter(Arrays.stream(args).map(String::toLowerCase).collect(Collectors.toList()))
        );
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
        this.sstables = Sets.newHashSet();

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
                        System.err.printf(CANNOT_FIND_FILE, sstable.getAbsolutePath());
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
            metadata = CassandraUtils.tableFromBestSource(sstables.iterator().next());
        }
    }

    public void doSchema(String command) throws Exception {
        String path = command.substring(6).trim().replaceAll("\"", "");

        if (path.equalsIgnoreCase("off")) {
            System.out.println(DISABLING_SCHEMA);
            CassandraUtils.cqlOverride = null;
        } else if (Strings.isNullOrEmpty(path)) {
            if (!Strings.isNullOrEmpty(CassandraUtils.cqlOverride)) {
                System.out.printf(USER_DEFINED_SCHEMA, CassandraUtils.cqlOverride);
            } else {
                System.out.println(NO_USER_DEFINED_SCHEMA);
            }
        } else {
            File schemaFile = new File(path);
            if (!schemaFile.exists()) {
                System.err.printf(CANNOT_FIND_FILE, schemaFile.getAbsolutePath());
            } else {
                String cql = new String(Files.readAllBytes(schemaFile.toPath()));
                try {
                    ParsedStatement statement = QueryProcessor.parseStatement(cql);
                    if (statement instanceof CreateTableStatement.RawStatement) {
                        CassandraUtils.cqlOverride = cql;
                        System.out.printf(IMPORTED_SCHEMA, schemaFile.getAbsolutePath());
                    } else {
                        System.err.printf(FAILED_TO_IMPORT_SCHEMA, schemaFile.getAbsoluteFile(), "Wrong type of statement, " + statement.getClass());
                    }
                } catch (SyntaxException se) {
                    System.err.printf(FAILED_TO_IMPORT_SCHEMA, schemaFile.getAbsoluteFile(), se.getMessage());
                }
            }
        }

    }

    public void doDump(String command) throws Exception {
        if (sstables.isEmpty()) {
            System.out.println(MISSING_SSTABLES);
            return;
        }
        Query query;
        if (command.length() > 5) {
            query = getQuery("select * from sstables " + command.substring(5));
        } else {
            query = getQuery("select * from sstables");
        }

        console.setHistoryEnabled(false);
        AtomicInteger totalRows = new AtomicInteger(0);
        try (UnfilteredPartitionIterator scanner = query.getScanner()) {
            int limit = Integer.MAX_VALUE;
            if (query.statement.limit != null && !query.selection.isAggregate()) {
                limit = Integer.parseInt(query.statement.limit.getText());
            }
            AtomicInteger rowsPaged = new AtomicInteger(0);
            Function<Void, Void> deferToInput = o -> {
                if (paging && rowsPaged.get() >= pageSize) {
                    rowsPaged.set(0);
                    try {
                        String input = console.readLine("\n---MORE---", ' ');
                        if (input == null) {
                            totalRows.set(Integer.MAX_VALUE);
                        }
                    } catch (UserInterruptException uie) {
                        // User interrupted, stop paging.
                        totalRows.set(Integer.MAX_VALUE);
                    } catch (IOException e) {
                        System.err.println(errorMsg("Error while paging:" + e.getMessage()));
                    }
                }
                return null;
            };

            while (scanner.hasNext() && totalRows.get() < limit) {
                UnfilteredRowIterator partition = scanner.next();
                deferToInput.apply(null);
                if (!partition.partitionLevelDeletion().isLive() && totalRows.get() < limit) {
                    System.out.println("[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "] " +
                            partition.partitionLevelDeletion());
                    rowsPaged.incrementAndGet();
                    totalRows.incrementAndGet();
                }
                deferToInput.apply(null);
                if (!partition.staticRow().isEmpty() && totalRows.get() < limit) {
                    System.out.println(
                            "[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "] " +
                                    partition.staticRow().toString(metadata, true));
                    rowsPaged.incrementAndGet();
                    totalRows.incrementAndGet();
                }
                deferToInput.apply(null);
                while (partition.hasNext() && totalRows.get() < limit) {
                    Unfiltered row = partition.next();
                    System.out.println(
                            "[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "] " +
                                    row.toString(metadata, true));
                    rowsPaged.incrementAndGet();
                    totalRows.incrementAndGet();
                    deferToInput.apply(null);
                }
            }
        } finally {
            if (totalRows.get() < Integer.MAX_VALUE) {
                System.out.printf("%n(%s rows)%n", totalRows.get());
            }
            console.setHistoryEnabled(true);
            console.setPrompt(prompt);
        }
    }

    public void doQuery(String command) throws Exception {
        Query q = getQuery(command);
        System.out.println();
        if (q == null) {
            System.out.println(MISSING_SSTABLES);
        } else if (paging) {
            ResultSetData resultData = getQuery(command).getResults(pageSize);
            TableTransformer.dumpResults(metadata, resultData.getResultSet(), System.out);
            boolean terminated = false;
            if (resultData.getPagingData().hasMorePages()) {
                console.setHistoryEnabled(false);
                while (resultData.getPagingData().hasMorePages()) {
                    try {
                        String input = console.readLine("\n---MORE---", ' ');
                        if (input == null) {
                            done = true;
                            terminated = true;
                            break;
                        }
                    } catch (UserInterruptException uie) {
                        // User interrupted, stop paging.
                        terminated = true;
                        break;
                    }
                    resultData = getQuery(command).getResults(pageSize, resultData.getPagingData());
                    TableTransformer.dumpResults(metadata, resultData.getResultSet(), System.out);
                }
            }
            if (!terminated) {
                System.out.printf("%n(%s rows)%n", resultData.getPagingData().getRowCount());
            }
            console.setHistoryEnabled(true);
            console.setPrompt(prompt);
        } else {
            ResultSetData resultData = getQuery(command).getResults();
            TableTransformer.dumpResults(metadata, resultData.getResultSet(), System.out);
            System.out.printf("%n(%s rows)%n", resultData.getPagingData().getRowCount());
        }
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
                        System.out.println(ANSI_RED + e.getMessage() + ANSI_RESET);
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

    public void doPagingConfig(String command) throws Exception {
        String mode = command.substring(6).trim().toLowerCase();
        switch (mode) {
            case "":
                if (paging) {
                    System.out.printf(PAGING_IS_ENABLED, pageSize);
                } else {
                    System.out.println(PAGING_IS_DISABLED);
                }
                break;
            case "on":
                if (paging) {
                    System.err.println(QUERY_PAGING_ALREADY_ENABLED);
                } else {
                    paging = true;
                    System.out.printf(QUERY_PAGING_ENABLED, pageSize);
                }
                break;
            case "off":
                if (!paging) {
                    System.err.println(QUERY_PAGING_ALREADY_DISABLED);
                } else {
                    paging = false;
                    System.out.println(QUERY_PAGING_DISABLED);
                }
                break;
            default:
                try {
                    pageSize = Integer.parseInt(mode);
                    paging = true;
                    System.out.printf(QUERY_PAGING_ENABLED, pageSize);
                } catch (NumberFormatException e) {
                    System.err.println(IMPROPER_PAGING_COMMAND);
                }
        }
    }

    public void doPersistConfig(String command) {
        String mode = command.substring(7).trim().toLowerCase();

        switch (mode) {
            case "":
                if (preferences) {
                    System.out.printf(PREFERENCES_ARE_ENABLED, generatePreferencesConfigString(false));
                } else {
                    System.out.println(PREFERENCES_ARE_DISABLED);
                }
                break;
            case "on":
                if (preferences) {
                    System.err.println(PREFERENCES_ALREADY_ENABLED);
                } else {
                    preferences = true;
                    System.out.printf(PREFERENCES_ENABLED, generatePreferencesConfigString(false));
                }
                break;
            case "off":
                if (preferences) {
                    preferences = false;
                    System.out.println(PREFERENCES_DISABLED);
                } else {
                    System.err.println(PREFERENCES_ALREADY_DISABLED);
                }
                break;
        }
    }

    public Query getQuery(String command) throws Exception {
        SelectStatement.RawStatement statement = (SelectStatement.RawStatement) QueryProcessor.parseStatement(command);
        if (statement.columnFamily().matches("sstables?")) {
            if (sstables.isEmpty()) {
                return null;
            }
            metadata = CassandraUtils.tableFromBestSource(sstables.iterator().next());
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
            if (command.isEmpty()) {
                continue;
            } else if (command.equals("exit") || command.equals("quit")) {
                done = true;
                continue;
            } else if (command.toLowerCase().trim().startsWith("describe schema")) {
                if (CassandraUtils.cqlOverride != null) {
                    System.out.println(CassandraUtils.cqlOverride);
                } else if (metadata != null) {
                    System.out.println(metadata);
                } else {
                    System.err.format("%sNo current metadata set, use a CREATE TABLE statement to set%s%n",
                            ANSI_RED, ANSI_RESET);
                }
                continue;
            } else if (command.toLowerCase().trim().startsWith("describe sstable")) {
                System.out.println();
                for (File f : sstables) {
                    System.out.println("\u001B[1;34m" + f.getAbsolutePath());
                    System.out.println(ANSI_CYAN + Strings.repeat("=", f.getAbsolutePath().length()) + ANSI_RESET);
                    CassandraUtils.printStats(f.getAbsolutePath(), System.out, console);
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
            } else if (command.toLowerCase().startsWith("persist")) {
                doPersistConfig(command);
                continue;
            } else if (command.length() >= 6) {
                String queryType = command.substring(0, 6).toLowerCase();
                switch (queryType) {
                    case "select":
                        doQuery(command);
                        continue;
                    case "create":
                        doCreate(command);
                        continue;
                    case "update":
                    case "insert":
                    case "delete":
                        System.err.format("%sQuery '%s' is not supported since this tool is read-only.%s%n",
                                ANSI_RED, command, ANSI_RESET);
                        continue;
                    case "paging":
                        doPagingConfig(command);
                        continue;
                    case "schema":
                        doSchema(command);
                        continue;
                }
            }
            System.err.format("%sUnknown command: %s%s%n", ANSI_RED, command, ANSI_RESET);
        }
    }


    public Config generatePreferencesConfig() {
        Config persistConfig;

        if (preferences) {
            persistConfig = this.config
                    .withValue(PROP_SSTABLES, ConfigValueFactory.fromIterable(sstables.stream().map(File::getAbsolutePath).collect(Collectors.toSet())))
                    .withValue(PROP_PAGING_ENABLED, ConfigValueFactory.fromAnyRef(paging))
                    .withValue(PROP_PAGING_SIZE, ConfigValueFactory.fromAnyRef(pageSize))
                    .withValue(PROP_PREFERENCES_ENABLED, ConfigValueFactory.fromAnyRef(preferences))
                    .withValue(PROP_SCHEMA, ConfigValueFactory.fromAnyRef(CassandraUtils.cqlOverride != null ? CassandraUtils.cqlOverride : ""));
        } else {
            persistConfig = ConfigFactory.empty()
                    .withValue(PROP_PREFERENCES_ENABLED, ConfigValueFactory.fromAnyRef(preferences));
        }

        return persistConfig;
    }

    public String generatePreferencesConfigString(boolean json) {
        return generatePreferencesConfig().root().render(ConfigRenderOptions.defaults().setJson(json).setOriginComments(false));
    }

    public void persistState() {
        String contents = generatePreferencesConfigString(true);
        try {
            Files.write(Paths.get(PREFERENCES_FILE.getAbsolutePath()), contents.getBytes());
        } catch (IOException e) {
            System.err.printf(errorMsg("Failed writing preferences file '%s': %s%n"),
                    PREFERENCES_FILE.getAbsolutePath(), e.getMessage());
        }
    }

    public static void main(String args[]) {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.format("%sFailure parsing arguments: %s%s%n%n", ANSI_RED, e.getMessage(), ANSI_RESET);
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
                    sh.persistState();
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
                } catch (RuntimeException e) {
                    if (e.getMessage().startsWith("Unknown column")) {
                        System.out.printf("%sUnknown Column. Likely the schema is not correct for this sstable. Try '%sSCHEMA OFF%s' to use metadata from statistics.%s%n", ANSI_RED, "\u001B[1;31m", "\u001B[0;31m", ANSI_RESET);
                    } else {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            sh.console.shutdown();
        }

    }
}
