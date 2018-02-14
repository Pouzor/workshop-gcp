package com.eleven.workshop.jobs;

import com.eleven.workshop.ExportToMongoOptions;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportToMongo {

    public static final Logger LOGGER = LoggerFactory.getLogger(ExportToMongo.class);

    public static void main(String[] args) {
        ExportToMongoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExportToMongoOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollection<String> datas = p.apply("Read JSON", TextIO.read().from(options.getFilePath()));
        PCollection<TableRow> datasTableRow = datas.apply("Transform Json to Tablerow", ParDo.of(new JSONParser()));


        campaignJson.apply("Write file campaign", TextIO.write().to("gs://xxxxx/xxxxx").withoutSharding().withSuffix(".json"));

        p.run().waitUntilFinish();
    }

}
