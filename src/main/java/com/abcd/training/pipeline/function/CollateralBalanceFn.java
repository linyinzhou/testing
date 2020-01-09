package com.abcd.training.pipeline.function;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import com.abcd.training.pipeline.entity.CompositeID;
import com.abcd.training.pipeline.entity.LegalDoc;

public class CollateralBalanceFn
		extends DoFn<KV<CompositeID, Map<String, double[]>>, KV<CompositeID, Map<String, double[]>>> {
	private static final long serialVersionUID = 7896532371396572180L;

	private PCollectionView<Map<String, LegalDoc>> legalDocMap;

	public CollateralBalanceFn(PCollectionView<Map<String, LegalDoc>> legalDocMap) {
		this.legalDocMap = legalDocMap;
	}

	@ProcessElement
	public void processElement(@Element KV<CompositeID, Map<String, double[]>> input,
			OutputReceiver<KV<CompositeID, Map<String, double[]>>> out, ProcessContext c) {
		Map<String, LegalDoc> legalDoctoCptyMap = c.sideInput(legalDocMap);
		double collateralBalance = Double
				.parseDouble(legalDoctoCptyMap.get(input.getKey().getLegalDocId()).getCollateralBalance());
		Map<String, double[]> newResult = new HashMap<>();
		input.getValue().keySet().forEach(timestep -> {
			double[] timestepResult = input.getValue().get(timestep);
			double[] result = new double[timestepResult.length];
			Arrays.parallelSetAll(result, i -> timestepResult[i] - collateralBalance);
			newResult.put(timestep, result);
		});
		out.output(KV.of(input.getKey(), newResult));
	}

}
