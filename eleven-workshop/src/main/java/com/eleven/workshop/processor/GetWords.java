package com.eleven.workshop.processor;


import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.DoFn;
import java.io.UnsupportedEncodingException;

public class GetWords extends DoFn<String, String> {

    public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    @ProcessElement
    public void processElement(ProcessContext c) throws CoderException, UnsupportedEncodingException {

        String element = c.element();

        String[] words = c.element().split(TOKENIZER_PATTERN);

        for (String word : words) {
            if (!word.isEmpty()) {
                c.output(word);
            }
        }

    }
}