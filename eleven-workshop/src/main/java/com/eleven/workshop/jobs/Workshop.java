package com.eleven.workshop.jobs;

import com.eleven.workshop.common.WorkshopOptions;
import com.eleven.workshop.processor.GetWords;
import com.eleven.workshop.processor.ToBQProcessor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Workshop {

    public static final Logger LOGGER = LoggerFactory.getLogger(WorkshopOptions.class);


    public static void main(String[] args) {
        WorkshopOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WorkshopOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollection<String> lines = p.apply("Read Files", TextIO.read().from(options.getFilePath()));

        PCollection<String> words = lines.apply(
                ParDo.of(new GetWords()));

        PCollection<KV<String, Long>> wordCounts =
                words.apply(Count.<String>perElement());

        PCollection<String> datas = wordCounts.apply(ParDo.of(new ToBQProcessor()));

        datas.apply("Write file", TextIO.write().to("gs://xxxxx/xxxxx").withoutSharding().withSuffix(".json"));

        p.run().waitUntilFinish();
    }

}
