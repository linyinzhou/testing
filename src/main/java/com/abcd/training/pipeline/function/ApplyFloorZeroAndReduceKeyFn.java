package com.abcd.training.pipeline.function;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.abcd.training.pipeline.entity.CompositeID;

public class ApplyFloorZeroAndReduceKeyFn
		extends DoFn<KV<CompositeID, Map<String, double[]>>, KV<CompositeID, Map<String, double[]>>> {

	private static final long serialVersionUID = 8013226786592760648L;

	@ProcessElement
	public void processElement(@Element KV<CompositeID, Map<String, double[]>> input,
			OutputReceiver<KV<CompositeID, Map<String, double[]>>> out) {
		Map<String, double[]> newResult = new HashMap<>();
		input.getValue().keySet().forEach(timestep -> {
			double[] timestepResult = input.getValue().get(timestep);
			double[] result = new double[timestepResult.length];
			Arrays.parallelSetAll(result, i -> timestepResult[i] > 0 ? timestepResult[i] : 0);
			newResult.put(timestep, result);
		});
		out.output(KV.of(new CompositeID(null, null, input.getKey().getCptyId()), newResult));
	}

}
