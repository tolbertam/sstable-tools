package com.csforge.sstable.json;

import java.lang.reflect.Type;

import com.google.gson.*;
import org.apache.cassandra.db.rows.Cell;

public class CellSerializer implements JsonSerializer<Cell> {

    private static final CellSerializer instance = new CellSerializer();

    private CellSerializer() {}

    @Override
    public JsonElement serialize(Cell cell, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject json = new JsonObject();
        json.add("name", new JsonPrimitive(cell.column().name.toCQLString()));

        if(cell.isTombstone()) {
            json.add("deletion_time", new JsonPrimitive(cell.localDeletionTime()));
        } else {
            json.add("value", new JsonPrimitive(cell.column().cellValueType().getString(cell.value())));
            if(cell.isExpiring()) {
                json.add("ttl", new JsonPrimitive(cell.ttl()));
                json.add("deletion_time", new JsonPrimitive(cell.localDeletionTime()));
            }
        }
        json.add("tstamp", new JsonPrimitive(cell.timestamp()));

        return json;
    }

    public static CellSerializer get() {
        return instance;
    }
}
