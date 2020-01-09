package com.abcd.training.pipeline.function;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

public class SumTradeResultFn extends CombineFn<Map<String, double[]>, SumTradeResultFn.Accum, Map<String, double[]>> {
	private static final long serialVersionUID = -6212468039609408782L;

	@Override
	public Accum addInput(Accum accumulator, Map<String, double[]> input) {
		accumulator.getTradeList().add(input);
		return accumulator;
	}

	@Override
	public Accum createAccumulator() {
		return new Accum();
	}

	@Override
	public Map<String, double[]> extractOutput(Accum accumulator) {
		Map<String, double[]> results = new HashMap<>();
		accumulator.getTradeList().forEach(tradeResultMap -> {
			tradeResultMap.keySet().forEach(timestep -> {
				double[] newArray = results.get(timestep);
				if (newArray == null) {
					results.put(timestep, tradeResultMap.get(timestep));
				} else {
					Arrays.parallelSetAll(newArray, i -> newArray[i] + tradeResultMap.get(timestep)[i]);
					results.put(timestep, newArray);
				}
			});
		});
		return results;
	}

	@Override
	public Accum mergeAccumulators(Iterable<Accum> accumulators) {
		Accum merged = createAccumulator();
		accumulators.forEach(accum -> merged.getTradeList().addAll(accum.getTradeList()));
		return merged;
	}

	@DefaultCoder(AvroCoder.class)
	class Accum implements Serializable {

		private static final long serialVersionUID = 1L;

		List<Map<String, double[]>> tradeList = new ArrayList<>();

		public List<Map<String, double[]>> getTradeList() {
			return tradeList;
		}

		public void setTradeList(List<Map<String, double[]>> tradeList) {
			this.tradeList = tradeList;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((tradeList == null) ? 0 : tradeList.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Accum other = (Accum) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (tradeList == null) {
				if (other.tradeList != null)
					return false;
			} else if (!tradeList.equals(other.tradeList))
				return false;
			return true;
		}

		private SumTradeResultFn getOuterType() {
			return SumTradeResultFn.this;
		}

	}

}
