package com.abcd.training.pipeline.transform;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.abcd.training.pipeline.entity.CombinedTradeResult;
import com.abcd.training.pipeline.entity.TradeResult;
import com.abcd.training.pipeline.function.CombineTradeResultFn;

public class CombineTimestepTradeResultCompositeTransform
		extends PTransform<PCollection<TradeResult>, PCollection<KV<String, CombinedTradeResult>>> {

	private static final long serialVersionUID = 1L;

	@Override
	public PCollection<KV<String, CombinedTradeResult>> expand(PCollection<TradeResult> input) {
		return input.apply("Key by trade ID", ParDo.of(new DoFn<TradeResult, KV<String, TradeResult>>() {
			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void processElement(@Element TradeResult input, OutputReceiver<KV<String, TradeResult>> out) {
				out.output(KV.of(input.getTradeId(), input));
			}
		})).apply("Collect Trade Results", Combine.perKey(new CombineTradeResultFn()));
	}
}
