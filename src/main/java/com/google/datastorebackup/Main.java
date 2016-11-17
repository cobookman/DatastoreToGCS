package com.google.datastorebackup;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreV1;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.protobuf.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;


public class Main {
  public static void main(String[] args) {
    System.out.println("Making Catalog->BigQuery pipeline");
    
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);
    
    if (options.getIsBlocking()) {
      options.setRunner(BlockingDataflowPipelineRunner.class);
    } else {
      options.setRunner(DataflowPipelineRunner.class);
      options.setStreaming(false);
    }

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
            .apply("EntityToJson", ParDo.of(new DatastoreToJson()))
            .apply("WriteJson", TextIO.Write.to(options.getOutputLocation())
                .withSuffix(".json"));
    
    System.out.println("Running pipeline");
    pipeline.run();
  }

  private interface Options extends DataflowPipelineOptions {
    @Description("Datastore entity kind")
    @Validation.Required
    String getDatastoreEntityKind();
    void setDatastoreEntityKind(String datastoreEntityKind);

    @Description("Output GCS Filename")
    @Validation.Required
    String getOutputLocation();
    void setOutputLocation(String outputLocation);
    
    @Description("Block until dataflow job finishes")
    boolean getIsBlocking();
    void setIsBlocking(boolean isBlocking);
  }

  static class DatastoreToJson extends DoFn<Entity, String> {
    private static Gson gson = new Gson();

    @Override
    public void processElement(DoFn<Entity, String>.ProcessContext context) throws Exception {
      Object record = entityToObject(context.element());
      String output = gson.toJson(record);
      context.output(output);
    }

    public HashMap<String, Object> entityToObject(Entity e) {
      Map<String, Value> fields = e.getProperties();
      HashMap<String, Object> out = new HashMap<String, Object>();
      for (String key : fields.keySet()) {
        Value v = fields.get(key);
        out.put(key, valueToObject(v));
      }
      return out;
    }

    public Object valueToObject(Value v) throws UnsupportedOperationException {
      Object s = null;
      switch (v.getValueTypeCase()) {
        case STRING_VALUE:
          s = v.getStringValue();
          break;
        case BOOLEAN_VALUE:
          s = Boolean.valueOf(v.getBooleanValue());
          break;
        case DOUBLE_VALUE:
          s = Double.valueOf(v.getDoubleValue());
          break;
        case TIMESTAMP_VALUE:
          Timestamp t = v.getTimestampValue();
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
          sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
          Long seconds = t.getSeconds();
          Long milliseconds = (long) (t.getNanos() / 1000000);
          s = sdf.format(new Date(seconds * 1000 + milliseconds));
          break;
        case INTEGER_VALUE:
          s = Long.valueOf(v.getIntegerValue());
          break;
        case ENTITY_VALUE:
          s = entityToObject(v.getEntityValue());
          break;
        case ARRAY_VALUE:
          ArrayList<Object> vals = new ArrayList<Object>();
          for (Value subval : v.getArrayValue().getValuesList()) {
            vals.add(valueToObject(subval));
          }
          s = vals;
          break;
        case NULL_VALUE:
          s = null;
          break;
        case KEY_VALUE:
          throw new UnsupportedOperationException("KEY_VALUE, is not currently supported");
        case BLOB_VALUE:
          throw new UnsupportedOperationException("BLOB_VALUE, is not currently supported");
        case GEO_POINT_VALUE:
          throw new UnsupportedOperationException("GEO_POINT_VALUE, is not currently supported");
        case VALUETYPE_NOT_SET:
          throw new UnsupportedOperationException("VALUETYPE_NOT_SET, is not not currently supported");
      }
      return s;
    }
  }
}
