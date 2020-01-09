package com.abcd.training.pipeline.entity;

import java.io.Serializable;

public class TradeResult implements Serializable {

	private static final long serialVersionUID = 6947240455576700885L;

	private String tradeId;

	private String timeStep;

	private String result;

	public TradeResult(String tradeId, String timeStep, String result) {
		super();
		this.tradeId = tradeId;
		this.timeStep = timeStep;
		this.result = result;
	}

	@Override
	public String toString() {
		return "TradeResult [tradeId=" + tradeId + ", timeStep=" + timeStep + ", result=" + result + "]";
	}

	public String getTradeId() {
		return tradeId;
	}

	public void setTradeId(String tradeId) {
		this.tradeId = tradeId;
	}

	public String getTimeStep() {
		return timeStep;
	}

	public void setTimeStep(String timeStep) {
		this.timeStep = timeStep;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.result == null) ? 0 : this.result.hashCode());
		result = prime * result + ((timeStep == null) ? 0 : timeStep.hashCode());
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
		TradeResult other = (TradeResult) obj;
		if (result == null) {
			if (other.result != null)
				return false;
		} else if (!result.equals(other.result))
			return false;
		if (timeStep == null) {
			if (other.timeStep != null)
				return false;
		} else if (!timeStep.equals(other.timeStep))
			return false;
		if (tradeId == null) {
			if (other.tradeId != null)
				return false;
		} else if (!tradeId.equals(other.tradeId))
			return false;
		return true;
	}
}
