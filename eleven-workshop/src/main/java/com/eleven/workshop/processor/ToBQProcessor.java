package com.eleven.workshop.processor;


import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;

import java.io.UnsupportedEncodingException;

public class ToBQProcessor extends DoFn<KV<String, Long>, String> {


    @ProcessElement
    public void processElement(ProcessContext c) throws CoderException, UnsupportedEncodingException {

        KV<String, Long> element = c.element();

        TableRow tablerow = new TableRow();

        tablerow.set("nb", element.getValue());
        tablerow.set("word", element.getKey());

        String json = new String(CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), tablerow), "UTF-8");

        c.output(json);

    }
}