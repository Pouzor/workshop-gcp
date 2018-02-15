package com.eleven.workshop.jobs;

import com.eleven.workshop.common.WorkshopOptions;

import com.eleven.workshop.processor.GetWords;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Workshop {

    public static final Logger LOGGER = LoggerFactory.getLogger(WorkshopOptions.class);

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    public static void main(String[] args) {
        WorkshopOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WorkshopOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollection<String> lines = p.apply("Read Files", TextIO.read().from(options.getFilePath()));
        
        PCollection<String> words = lines.apply(
                ParDo.of(new GetWords()));

        PCollection<KV<String, Long>> wordCounts =
                words.apply(Count.<String>perElement());

        PCollection<String> map = wordCounts.apply(MapElements.via(new FormatAsTextFn()));

        map.apply("Write file", TextIO.write().to("gs://xxxxx/xxxxx").withoutSharding().withSuffix(".txt"));

        p.run().waitUntilFinish();
    }

}
