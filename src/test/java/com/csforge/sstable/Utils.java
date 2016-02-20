package com.csforge.sstable;

import com.google.common.io.ByteStreams;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URLClassLoader;

public class Utils {

    private static File copyResource(String name) throws Exception {
        InputStream is = URLClassLoader.getSystemResourceAsStream(name);
        String tempDir = System.getProperty("java.io.tmpdir");
        File tmp = new File(tempDir + File.separator + name);
        tmp.deleteOnExit();
        ByteStreams.copy(is, new FileOutputStream(tmp));
        return tmp;
    }

    public static File getDB(int generation) throws Exception {
        copyResource("ma-" + generation + "-big-Digest.crc32");
        copyResource("ma-" + generation + "-big-TOC.txt");
        copyResource("ma-" + generation + "-big-CompressionInfo.db");
        copyResource("ma-" + generation + "-big-Filter.db");
        copyResource("ma-" + generation + "-big-Index.db");
        copyResource("ma-" + generation + "-big-Statistics.db");
        copyResource("ma-" + generation + "-big-Summary.db");
        copyResource("ma-" + generation + "-big-TOC.txt");
        return copyResource("ma-" + generation + "-big-Data.db");
    }
}
