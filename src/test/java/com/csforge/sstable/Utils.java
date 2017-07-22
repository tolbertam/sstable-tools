package com.csforge.sstable;

import com.google.common.io.ByteStreams;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URLClassLoader;

public class Utils {
    public static String CQL1 =
            "    CREATE TABLE composites (\n" +
                    "        key1 varchar,\n" +
                    "        key2 varchar,\n" +
                    "        ckey1 varchar,\n" +
                    "        ckey2 varchar,\n" +
                    "        value bigint,\n" +
                    "        PRIMARY KEY((key1, key2), ckey1, ckey2)\n" +
                    "    );";

    public static String CQL2 =
            "    CREATE TABLE blog.users (\n" +
                    "        user_name varchar PRIMARY KEY,\n" +
                    "        password varchar,\n" +
                    "        gender varchar,\n" +
                    "        state varchar,\n" +
                    "        birth_year bigint\n" +
                    "    );";

    public static String CQL3 = "CREATE TABLE IF NOT EXISTS test.wide ( key text, key2 text, val text, PRIMARY KEY (key, key2));";
    public static String CQL4 = "CREATE TABLE collections (key1 varchar, listval list<text>, mapval map<text, text>, setval set<text>, PRIMARY KEY (key1))";

    public static String CQL5 =
            "CREATE TABLE temperature_by_day(\n" +
                    "        weatherstation_id text\n," +
                    "        date text\n," +
                    "        event_time timestamp,\n" +
                    "        temperature float,\n" +
                    "        PRIMARY KEY ((weatherstation_id,date),event_time)\n" +
                    ");";

    private static File copyResource(String name) throws Exception {
        InputStream is = URLClassLoader.getSystemResourceAsStream(name);
        String tempDir = System.getProperty("java.io.tmpdir");
        File tmp = new File(tempDir + File.separator + name);
        tmp.deleteOnExit();
        ByteStreams.copy(is, new FileOutputStream(tmp));
        return tmp;
    }

    public static File getSSTable(String version, int generation) throws Exception {
        copyResource(version + "-" + generation + "-big-Digest.crc32");
        copyResource(version + "-" + generation + "-big-TOC.txt");
        copyResource(version + "-" + generation + "-big-CompressionInfo.db");
        copyResource(version + "-" + generation + "-big-Filter.db");
        copyResource(version + "-" + generation + "-big-Index.db");
        copyResource(version + "-" + generation + "-big-Statistics.db");
        copyResource(version + "-" + generation + "-big-Summary.db");
        copyResource(version + "-" + generation + "-big-TOC.txt");
        return copyResource(version + "-" + generation + "-big-Data.db");
    }
}
