/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.abcd.training.pipeline;

import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abcd.training.pipeline.entity.CombinedTradeResult;
import com.abcd.training.pipeline.entity.CompositeID;
import com.abcd.training.pipeline.entity.LegalDoc;
import com.abcd.training.pipeline.entity.Trade;
import com.abcd.training.pipeline.function.ApplyFloorZeroAndReduceKeyFn;
import com.abcd.training.pipeline.function.AverageOverScenarioFn;
import com.abcd.training.pipeline.function.CollateralBalanceFn;
import com.abcd.training.pipeline.function.LoggingFn;
import com.abcd.training.pipeline.function.ParseLegalDocFn;
import com.abcd.training.pipeline.function.ParseTradeAttributeFn;
import com.abcd.training.pipeline.function.SumTradeResultFn;
import com.abcd.training.pipeline.function.TradeResultFn;
import com.abcd.training.pipeline.transform.CombineTimestepTradeResultCompositeTransform;
import com.abcd.training.pipeline.transform.SplitTradeByLegalDocCompositeTransform;

public class NormalFlowPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(NormalFlowPipeline.class);

	public static void main(String[] args) {
		PipelineOptionsFactory.register(CustomOptions.class);
		CustomOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		
		//parse source file
		PCollection<String> result = pipeline.apply("Read TradeResult", TextIO.read().from(options.getTradeResult()));
		PCollection<String> trade = pipeline.apply("Read TradeAttribute", TextIO.read().from(options.getTrade()));
		PCollection<String> legalDoc = pipeline.apply("Read LegalDoc", TextIO.read().from(options.getLegalDoc()));

		//source to map, trade id - trade
		PCollectionView<Map<String, Trade>> tradeMap = trade
				.apply("Parse Line to Trade Map", ParDo.of(new ParseTradeAttributeFn()))
				.apply("View.asMap", View.asMap());
		
		//legaldoc id - legaldoc
		PCollectionView<Map<String, LegalDoc>> legalDocMap = legalDoc
				.apply("Parse Line to LegalDoc Map", ParDo.of(new ParseLegalDocFn()))
				.apply("View.asMap", View.asMap());
		
		//trade id - trade result
		PCollection<KV<String, CombinedTradeResult>> tradeResult = result
				.apply("Parse Line to TradeResult", ParDo.of(TradeResultFn.readFile()))
				.apply(new CombineTimestepTradeResultCompositeTransform());

		TupleTag<KV<CompositeID, Map<String, double[]>>> nodoc = new TupleTag<KV<CompositeID, Map<String, double[]>>>() {
		};
		TupleTag<KV<CompositeID, Map<String, double[]>>> closeout = new TupleTag<KV<CompositeID, Map<String, double[]>>>() {
		};
		TupleTag<KV<CompositeID, Map<String, double[]>>> collateral = new TupleTag<KV<CompositeID, Map<String, double[]>>>() {
		};

		//construct the result by side input
		PCollectionTuple resultTuple = tradeResult
				.apply(new SplitTradeByLegalDocCompositeTransform(tradeMap, legalDocMap, nodoc, closeout, collateral));

		PCollection<KV<CompositeID, Map<String, double[]>>> nodocTradeSet = resultTuple.get(nodoc);
		PCollection<KV<CompositeID, Map<String, double[]>>> closeoutSet = resultTuple.get(closeout)
				.apply("Net Closeout", Combine.perKey(new SumTradeResultFn()));
		PCollection<KV<CompositeID, Map<String, double[]>>> collateralSet = resultTuple.get(collateral)
				.apply("Net Collateral", Combine.perKey(new SumTradeResultFn()))
				.apply("Apply Collateral Balance",ParDo.of(new CollateralBalanceFn(legalDocMap)).withSideInputs(legalDocMap));
		
		PCollection<KV<CompositeID, Map<String, double[]>>> mergedSet = PCollectionList.of(nodocTradeSet)
				.and(closeoutSet).and(collateralSet).apply(Flatten.pCollections());
		LOG.info("------------------------- Netting result -------------------------");
		mergedSet.apply(ParDo.of(new LoggingFn<KV<CompositeID, Map<String, double[]>>>("Merged Result")));
		
		PCollection<KV<CompositeID, Map<String, double[]>>> cptyLevelSet = mergedSet
				.apply("Apply floor = 0, reduce key to CptyId", ParDo.of(new ApplyFloorZeroAndReduceKeyFn()))
				.apply("Sum up to CptyLevel", Combine.perKey(new SumTradeResultFn()));
		LOG.info("------------------------- Cpty result -------------------------");
		cptyLevelSet.apply(ParDo.of(new LoggingFn<KV<CompositeID, Map<String, double[]>>>("Rollup to Cpty Result")));
		
		LOG.info("------------------------- Cpty average over scenario result -------------------------");
		cptyLevelSet.apply("Calculate average", ParDo.of(new AverageOverScenarioFn()))
				.apply(ParDo.of(new LoggingFn<KV<CompositeID, Map<String, double[]>>>("Final Result")));
		pipeline.run();
	}
}
