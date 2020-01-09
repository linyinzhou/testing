package com.abcd.training.pipeline.function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.abcd.training.pipeline.entity.Trade;

public class ParseTradeAttributeFn extends DoFn<String, KV<String, Trade>> {
	private static final long serialVersionUID = 5675714999554551838L;

	@ProcessElement
	public void processElement(@Element String input, OutputReceiver<KV<String, Trade>> out) {
		String[] values = input.split(",");
		Trade trade = new Trade(values[0], values[1]);
		out.output(KV.of(values[0], trade));
	}
}
