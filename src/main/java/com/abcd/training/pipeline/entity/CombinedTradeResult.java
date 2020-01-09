package com.abcd.training.pipeline.entity;

import java.io.Serializable;
import java.util.Map;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class CombinedTradeResult implements Serializable {

	private static final long serialVersionUID = -1259581521792049985L;
	@Nullable
	private String tradeId;
	@Nullable
	private Map<String, double[]> results;

	public CombinedTradeResult() {
		super();
	}

	public CombinedTradeResult(String tradeId, Map<String, double[]> results) {
		super();
		this.tradeId = tradeId;
		this.results = results;
	}

	public String getTradeId() {
		return tradeId;
	}

	public void setTradeId(String tradeId) {
		this.tradeId = tradeId;
	}

	public Map<String, double[]> getResults() {
		return results;
	}

	public void setResults(Map<String, double[]> results) {
		this.results = results;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((results == null) ? 0 : results.hashCode());
		result = prime * result + ((tradeId == null) ? 0 : tradeId.hashCode());
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
		CombinedTradeResult other = (CombinedTradeResult) obj;
		if (results == null) {
			if (other.results != null)
				return false;
		} else if (!results.equals(other.results))
			return false;
		if (tradeId == null) {
			if (other.tradeId != null)
				return false;
		} else if (!tradeId.equals(other.tradeId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CombinedTradeResult [tradeId=" + tradeId + ", results=" + results + "]";
	}

}
