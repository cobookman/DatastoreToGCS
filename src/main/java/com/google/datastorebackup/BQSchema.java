package com.google.datastorebackup;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;

public class BQSchema {
  public class ColumnSchema {
    String name;
    String type;
    List<ColumnSchema> record;
    boolean repeated;
    
    public ColumnSchema(String name, String type, Boolean repeated) {
      this.name = name;
      this.type = type;
      this.repeated = repeated;
      this.record = new ArrayList<>();
    }
  }
  
  public List<ColumnSchema> getSchema() {
    List<ColumnSchema> out = new ArrayList<>();
    out.add(new ColumnSchema("canvasId", "string", false));
    out.add(new ColumnSchema("drawingId", "string", false));
    
    ColumnSchema points = new ColumnSchema("points", "record", true);
    points.record.add(new ColumnSchema("x", "integer", false));
    points.record.add(new ColumnSchema("y", "integer", false));
    out.add(points);
    return out;
  } 

  // Creates a column table schema
  private static TableFieldSchema getTableField(ColumnSchema columnSchema) throws Exception {
    TableFieldSchema tableField = new TableFieldSchema()
        .setName(columnSchema.name);
    
    if (columnSchema.repeated) {
      tableField.setMode("REPEATED");
    }
    
    if (!columnSchema.type.equals("record")) {
      tableField.setType(columnSchema.type.toUpperCase());
    } else {
      tableField.setType("RECORD");
      List<TableFieldSchema> fields = new ArrayList<>();
      for (ColumnSchema recordSchema : columnSchema.record) {
        fields.add(getTableField(recordSchema));  
      }
      tableField.setFields(fields);
    } 
    return tableField;
  }
  
  // converts the inputed json to a bq table schema obj.
  public TableSchema getTableSchema() throws Exception {
    ArrayList<TableFieldSchema> tableFieldSchemas = new ArrayList<TableFieldSchema>();
    for (ColumnSchema columnSchema : new BQSchema().getSchema()) {
      tableFieldSchemas.add(getTableField(columnSchema));
    }
    tableFieldSchemas.add(new TableFieldSchema().setName("_key").setType("string"));
    return new TableSchema().setFields(tableFieldSchemas);
  }
}
