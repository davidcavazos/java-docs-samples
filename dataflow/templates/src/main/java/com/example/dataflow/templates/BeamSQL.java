// Copyright 2018 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.example.dataflow.templates;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BeamSQL {

  private static final Pattern tableSpecPattern = Pattern.compile(
      "(?<project>[^:]+):(?<dataset>[^.]+)\\.(?<table>.+)");

  interface BeamSQLOptions extends PipelineOptions {
    @Description("Source BigQuery table spec, format: project:dataset.table")
//    ValueProvider<String> getSourceTable();
//    void setSourceTable(ValueProvider<String> value);
    String getSourceTable();
    void setSourceTable(String value);

    @Description("Destination BigQuery table spec, format: project:dataset.table")
//    ValueProvider<String> getDestinationTable();
//    void setDestinationTable(ValueProvider<String> value);
    String getDestinationTable();
    void setDestinationTable(String value);

    @Description("Beam SQL query")
//    ValueProvider<String> getQuery();
//    void setQuery(ValueProvider<String> value);
    String getQuery();
    void setQuery(String value);
  }

  private static TableReference parseTableSpec(String tableSpec) {
    Matcher m = tableSpecPattern.matcher(tableSpec);
    if (!m.find())
      throw new IllegalArgumentException(
          String.format("Invalid table '%s', must be in the format 'project:dataset.table'", tableSpec));
    return new TableReference()
        .setProjectId(m.group("project"))
        .setDatasetId(m.group("dataset"))
        .setTableId(m.group("table"));
  }

  public static void main(String[] args) throws IOException {
    BeamSQLOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BeamSQLOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    // Validate input arguments.
    TableReference sourceTable = parseTableSpec(options.getSourceTable());
    TableReference destinationTable = parseTableSpec(options.getDestinationTable());

    // Get the source table schema.
//    Bigquery bigquery = BigqueryClient.getNewBigquerryClient("Beam SQL");
//    TableSchema schema = bigquery.tables()
//        .get(sourceTable.getProjectId(), sourceTable.getDatasetId(), sourceTable.getTableId())
//        .execute()
//        .getSchema();
//
//    List<TableFieldSchema> fields = schema.getFields();

    PCollection<TableRow> rows = pipeline
        .apply(BigQueryIO.readTableRows().from(options.getSourceTable()))
        .apply(MapElements.into(TypeDescriptor.of(TableRow.class))
            .via((TableRow row) -> {
              System.out.println(row.toString());
              return row;
            }));

    pipeline.run().waitUntilFinish();
  }
}
