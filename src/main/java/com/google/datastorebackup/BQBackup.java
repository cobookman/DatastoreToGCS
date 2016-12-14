package com.google.datastorebackup;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreV1;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.datastorebackup.BQSchema.ColumnSchema;
import com.google.datastore.v1.Key.PathElement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class BQBackup {

  
  
  public static void run(String[] args) throws Exception {
    System.out.println("Making Datastore->GCS pipeline");

    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    if (options.getIsBlocking()) {
      options.setRunner(BlockingDataflowPipelineRunner.class);
    } else {
      options.setRunner(DataflowPipelineRunner.class);
      options.setStreaming(false);
    }

    String date = (new SimpleDateFormat("yyyMMdd")).format(new Date());

    // make string in format: "my-project:dataset.entity_date"
    String tableName = String.format("%s:%s.%s_%s",
        options.getProject(),
        options.getBQDataset(),
        options.getDatastoreEntityKind(),
        date);
    System.out.println("Destination BigQuery Table is: " + tableName);

    // Build our Datastore query.
    // Right now we are simply grabbing all Datastore records of a given kind,
    Query.Builder queryBuilder = Query.newBuilder();
    queryBuilder.addKindBuilder().setName(options.getDatastoreEntityKind());
    Query query = queryBuilder.build();

    // Generate the Datastore Read Source
    DatastoreV1.Read read = DatastoreIO.v1().read()
        .withProjectId(options.getProject())
        .withQuery(query);
    
    // Build our data pipeline
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply("IngestEntities", read)
            .apply("EntityToTableRows", ParDo.of(new DatastoreToTableRow()))
            .apply("WriteTableRows", BigQueryIO.Write
                .to(tableName)
                .withSchema(new BQSchema().getTableSchema())
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    System.out.println("Running pipeline");
    pipeline.run();
  }
  

  
  private interface Options extends DataflowPipelineOptions {
    @Description("Datastore entity kind")
    @Validation.Required
    String getDatastoreEntityKind();
    void setDatastoreEntityKind(String datastoreEntityKind);

    @Description("BQ dataset")
    @Validation.Required
    String getBQDataset();
    void setBQDataset(String bqDataset);

    @Description("Block until dataflow job finishes")
    boolean getIsBlocking();
    void setIsBlocking(boolean isBlocking);
  }

  static class DatastoreToTableRow extends DoFn<Entity, TableRow> {

    // Takes in Datastore Entity pushes out table row
    @Override
    public void processElement(DoFn<Entity, TableRow>.ProcessContext context) throws Exception {
      System.out.println("Processing table row");
      Entity e = context.element();

      TableRow row = entityToRow(new BQSchema().getSchema(), e);
      System.out.println("key is" + getKeyString(e));
      row.put("_key", getKeyString(e));
      
      context.output(row);
    }
    
    // Gets the Key as a string from a datastore entity
    public String getKeyString(Entity e) {
      String out = "";
      for (PathElement pElm : e.getKey().getPathList()) {
        String part;
        if (pElm.getName() != null) {
          part = pElm.getName();
        } else {
          part = ((Long) pElm.getId()).toString();
        }
        out += "," + part;
      }
      return out.substring(1);
    }
    
    // Converts Entity to a table row
    public TableRow entityToRow(List<ColumnSchema> columnSchemas, Entity e) throws Exception {
      TableRow row = new TableRow();
      Map<String, Value> fields = e.getProperties();
           
      for (ColumnSchema columnSchema : columnSchemas) {
        Value v = fields.get(columnSchema.name);
        if (v != null) {
          Object column = valueToColumn(columnSchema, v);
          row.set(columnSchema.name, column);
        }
      }
      return row;
    }
    
    public Object valueToColumn(ColumnSchema columnSchema, Value v) throws Exception {
      Object out = null;
      
      switch (v.getValueTypeCase()) {
        case ARRAY_VALUE:
          List<Value> values = v.getArrayValue().getValuesList();
          if (v.getArrayValue() == null || v.getArrayValue().getValuesList() == null) {
            throw new Exception("ARRAY VALUE NULL");
          }
          ArrayList<Object> outValues = new ArrayList<>();
          for (Value arrValue : values) {
            outValues.add(valueToColumn(columnSchema, arrValue));
          }
          out = outValues;
          break;
          
        case ENTITY_VALUE:
          Entity e = v.getEntityValue();        
          out = entityToRow(columnSchema.record, e);
          break;
          
        case BOOLEAN_VALUE:
          if (columnSchema.type.equals("boolean")) {
            out = v.getBooleanValue();
          }
          break;
          
        case DOUBLE_VALUE:
          if (columnSchema.type.equals("float")) {
            out = v.getDoubleValue();
          }
          break;
          
        case INTEGER_VALUE:
          if (columnSchema.type.equals("integer")) {
            out = v.getIntegerValue();
          }
          break;
          
        case NULL_VALUE:
          out = null;
          break;
          
        case STRING_VALUE:
          if (columnSchema.type.equals("string")) {
            out = v.getStringValue();
          }
          break;
          
        case TIMESTAMP_VALUE:
          if (columnSchema.type.equals("datetime")) {
            out = v.getTimestampValue();
          }
          break;
          
        case KEY_VALUE:
          throw new UnsupportedOperationException("KEY_VALUE, is not currently supported");
        
        case BLOB_VALUE:
          throw new UnsupportedOperationException("BLOB_VALUE, is not currently supported");
        
        case GEO_POINT_VALUE:
          throw new UnsupportedOperationException("GEO_POINT_VALUE, is not currently supported");
       
        case VALUETYPE_NOT_SET:
          break;
       
        default:
          break;
      }
      return out;
    }
  }
}
