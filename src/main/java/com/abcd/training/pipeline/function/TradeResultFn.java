package com.abcd.training.pipeline.function;

import org.apache.beam.sdk.transforms.DoFn;

import com.abcd.training.pipeline.entity.TradeResult;

public class TradeResultFn extends DoFn<String, TradeResult> {

	private static final long serialVersionUID = -4142366830337566681L;

	private static TradeResultFn instance = new TradeResultFn();

	private TradeResultFn() {
	}

	public static TradeResultFn readFile() {
		return instance;
	}

	@ProcessElement
	public void processElement(@Element String input, OutputReceiver<TradeResult> out) {
		String[] values = input.split(",");
		if (!"TradeId".equalsIgnoreCase(values[0])) {
			String resultStr = "";
			for (int i = 2; i < values.length; i++) {
				if (i != values.length - 1) {
					resultStr += values[i] + ",";
				} else {
					resultStr += values[i];
				}
			}
			TradeResult traderesult = new TradeResult(values[0], values[1], resultStr);
			out.output(traderesult);
		}
	}
}
