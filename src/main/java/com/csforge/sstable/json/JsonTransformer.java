package com.csforge.sstable.json;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.stream.Stream;

import com.csforge.reader.Partition;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonTransformer {

    private static final Logger logger = LoggerFactory.getLogger(JsonTransformer.class);

    public static void toJson(Stream<Partition> partitions, CFMetaData metadata, OutputStream out) throws IOException {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(RowSerializer.class, new RowSerializer(metadata));
        gsonBuilder.registerTypeAdapter(TombstoneSerializer.class, new TombstoneSerializer(metadata));
        Gson gson = gsonBuilder.create();

        try(JsonWriter writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"))) {
            writer.setIndent("  ");
            writer.beginArray();
            partitions.forEach((partition) -> {
                String key = metadata.getKeyValidator().getString(partition.getKey().getKey());
                try {
                    /* TODO: Parse the individual key parts.
                    List<AbstractType<?>> keyParts = metadata.getKeyValidator().getComponents();
                    List<ColumnDefinition> partitionColumns = metadata.partitionKeyColumns();
                    assert keyParts.size() == partitionColumns.size();
                    writer.name("partition_key");
                    writer.beginObject();
                    for(int i = 0; i < keyParts.size(); i++) {
                        ColumnDefinition column = partitionColumns.get(i);
                        AbstractType<?> type = keyParts.get(i);
                        writer.name("name");
                        writer.value(column.name.toCQLString());
                        writer.name("value");
                        writer.value(type.getString());
                    }
                    writer.endObject();
                    */

                    writer.beginObject();

                    writer.name("key");
                    writer.value(key);

                    if(!partition.isLive()) {
                        writer.beginObject();
                        writer.name("deletion_time");
                        writer.value(partition.markedForDeleteAt());
                        writer.name("tstamp");
                        writer.value(partition.localDeletionTime());
                        writer.endObject();
                    }

                    if (!partition.staticRow().isEmpty()) {
                        gson.toJson(partition.staticRow(), Row.class, writer);
                    }

                    partition.rows().forEach(unfiltered -> {
                        if (unfiltered instanceof Row) {
                            gson.toJson(unfiltered, Row.class, writer);
                        } else {
                            gson.toJson(unfiltered, RangeTombstoneMarker.class, writer);
                        }
                    });

                    writer.endObject();
                } catch (IOException e) {
                    logger.error("Fatal error parsing partition: {}", key, e);
                }
            });
            writer.endArray();
        }
    }
}
