package com.abcd.training.pipeline.function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.abcd.training.pipeline.entity.LegalDoc;

public class ParseLegalDocFn extends DoFn<String, KV<String, LegalDoc>> {
	private static final long serialVersionUID = -5020189886183595553L;

	@ProcessElement
	public void processElement(@Element String input, OutputReceiver<KV<String, LegalDoc>> out) {
		String[] values = input.split(",");
		LegalDoc legalDoc = new LegalDoc(values[0], values[1], values[2], values[3], values[4]);
		out.output(KV.of(values[0], legalDoc));
	}
}
