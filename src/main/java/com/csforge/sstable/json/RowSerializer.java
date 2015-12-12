package com.csforge.sstable.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.rows.Row;

public class RowSerializer implements JsonSerializer<Row> {

    private final CFMetaData metadata;

    public RowSerializer(CFMetaData metadata) {
        this.metadata = metadata;
    }

    @Override
    public JsonElement serialize(Row row, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject json = new JsonObject();
        String rowType = row.isStatic() ? "static" : "row";
        json.add("type", new JsonPrimitive(rowType));

        LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
        if(!liveInfo.isEmpty()) {
            json.add("tstamp", new JsonPrimitive(liveInfo.timestamp()));
            if(liveInfo.isExpiring()) {
                json.add("ttl", new JsonPrimitive(liveInfo.ttl()));
                json.add("ttl_tstamp", new JsonPrimitive(liveInfo.localExpirationTime()));
                json.add("expired", new JsonPrimitive(liveInfo.isLive((int)System.currentTimeMillis() / 1000)));
            }
        }

        // Only print clustering information for non-static rows.
        if(!row.isStatic()) {
            JsonObject c = new JsonObject();
            Clustering clustering = row.clustering();
            List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
            assert clustering.size() == clusteringColumns.size();
            for(int i = 0; i < clustering.size(); i++) {
                ColumnDefinition column = clusteringColumns.get(i);
                c.add(column.name.toCQLString(), new JsonPrimitive(column.cellValueType().getString(clustering.get(i))));
            }
            json.add("clustering", c);
        }

        JsonArray columns = new JsonArray();
        JsonArray tombstones = new JsonArray();
        row.cells().forEach((cell) -> {
            JsonElement c = CellSerializer.get().serialize(cell, cell.getClass(), jsonSerializationContext);
            if (cell.isTombstone()) {
                tombstones.add(c);
            } else {
                columns.add(c);
            }
        });

        if(columns.size() > 0) {
            json.add("columns", columns);
        }
        if(tombstones.size() > 0) {
            json.add("tombstones", tombstones);
        }
        return json;
    }
}
