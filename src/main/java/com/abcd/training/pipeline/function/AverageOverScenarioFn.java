package com.abcd.training.pipeline.function;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.abcd.training.pipeline.entity.CompositeID;

public class AverageOverScenarioFn
		extends DoFn<KV<CompositeID, Map<String, double[]>>, KV<CompositeID, Map<String, double[]>>> {

	private static final long serialVersionUID = -4272737206551118607L;

	@ProcessElement
	public void processElement(@Element KV<CompositeID, Map<String, double[]>> input,
			OutputReceiver<KV<CompositeID, Map<String, double[]>>> out) {
		Map<String, double[]> newResult = new HashMap<>();
		input.getValue().keySet().forEach(timestep -> {
			double[] timestepResult = input.getValue().get(timestep);
			double[] result = new double[1];
			OptionalDouble averageResult = Arrays.stream(timestepResult).map(value -> value).average();
			result[0] = averageResult.getAsDouble();
			newResult.put(timestep, result);
		});
		out.output(KV.of(input.getKey(), newResult));
	}

}
