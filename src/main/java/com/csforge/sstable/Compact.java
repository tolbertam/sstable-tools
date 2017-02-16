package com.csforge.sstable;

import com.google.common.collect.Sets;
import javassist.runtime.Desc;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableTxnWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public class Compact {
    static {
        Config.setClientMode(true);
    }

    protected Collection<SSTableReader> sstables = Sets.newHashSet();
    protected CFMetaData metadata;

    public Compact(String... args) {
        for(String path : args) {
            try {
                for (File f : CassandraUtils.sstablesFromPath(path)) {
                    if (metadata == null) {
                        metadata = CassandraUtils.tableFromSSTable(f);
                    }
                    Descriptor d = Descriptor.fromFilename(f.getAbsolutePath());
                    sstables.add(SSTableReader.openNoValidation(d, metadata));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        try {
            Descriptor desc = new Descriptor(SSTableFormat.Type.BIG.info.getLatestVersion(),
                    new File("."),
                    "keyspace",
                    "table",
                    0,
                    SSTableFormat.Type.BIG,
                    Component.digestFor(BigFormat.latestVersion.uncompressedChecksumType()));

            SSTableTxnWriter out = SSTableTxnWriter.create(metadata,
                    desc,
                    0,
                    ActiveRepairService.UNREPAIRED_SSTABLE,
                    0,
                    SerializationHeader.make(metadata, sstables),
                    Collections.emptySet());

            System.out.println("Merging " + sstables.size() + " sstables to " + desc.filenameFor(Component.DATA));

            UnfilteredPartitionIterator merged =
                    UnfilteredPartitionIterators.mergeLazily(
                            sstables.stream()
                                    .map(SSTableReader::getScanner)
                                    .collect(Collectors.toList()),
                            FBUtilities.nowInSeconds());
            while (merged.hasNext()) {
                out.append(merged.next());
            }
            out.finish(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String ... args) {
        if (args.length == 0) {
            System.err.println("Usage: java -jar sstable-tools.jar compact [sstable ...]");
        } else {
            new Compact(args).run();
        }
    }

}
