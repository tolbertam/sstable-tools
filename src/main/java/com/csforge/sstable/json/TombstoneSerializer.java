package com.csforge.sstable.json;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;

public class TombstoneSerializer implements JsonSerializer<RangeTombstoneMarker> {

    private final CFMetaData metadata;

    public TombstoneSerializer(CFMetaData metadata) {
        this.metadata = metadata;
    }

    @Override
    public JsonElement serialize(RangeTombstoneMarker tombstone, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject json = new JsonObject();

        if(tombstone instanceof RangeTombstoneBoundMarker) {
            json.add("type", new JsonPrimitive("range_tombstone_bound"));
            RangeTombstoneBoundMarker bm = (RangeTombstoneBoundMarker)tombstone;
            RangeTombstone.Bound bound = bm.clustering();
            List<ColumnDefinition> clustering = metadata.clusteringColumns();
            StringBuilder data = new StringBuilder();
            for(int i = 0; i < clustering.size(); i++) {
                if(i != 0) {
                    data.append(':');
                }
                ColumnDefinition c = clustering.get(i);
                if(bound.size() > i) {
                    data.append(c.cellValueType().getString(bound.get(i)));
                } else {
                    data.append('*');
                }
            }
            String value = "?";
            switch(bound.kind()) {
                case EXCL_END_BOUND:
                    value = data.toString() + ')';
                    break;
                case INCL_START_BOUND:
                    value = '[' + data.toString();
                    break;
                case EXCL_END_INCL_START_BOUNDARY:
                    value = '[' + data.toString() + ')';
                    break;
                case STATIC_CLUSTERING:
                case CLUSTERING:
                    value = data.toString();
                    break;
                case INCL_END_EXCL_START_BOUNDARY:
                    value = '(' + data.toString() + ']';
                    break;
                case INCL_END_BOUND:
                    value = data.toString() + ']';
                    break;
                case EXCL_START_BOUND:
                    value = '(' + data.toString();
                    break;
            }
            json.add("value", new JsonPrimitive(value));
            json.add("deletion_time", new JsonPrimitive(bm.deletionTime().markedForDeleteAt()));
            json.add("tsamp", new JsonPrimitive(bm.deletionTime().localDeletionTime()));
        }
        return json;
    }
}
