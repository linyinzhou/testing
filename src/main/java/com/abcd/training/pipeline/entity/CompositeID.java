package com.abcd.training.pipeline.entity;

import java.io.Serializable;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class CompositeID implements Serializable {
	private static final long serialVersionUID = 3984583393364938174L;
	@Nullable
	private String tradeId;
	@Nullable
	private String legalDocId;
	@Nullable
	private String cptyId;

	public CompositeID() {
		super();
	}

	public CompositeID(String tradeId, String legalDocId, String cptyId) {
		super();
		this.tradeId = tradeId;
		this.legalDocId = legalDocId;
		this.cptyId = cptyId;
	}

	public String getTradeId() {
		return tradeId;
	}

	public void setTradeId(String tradeId) {
		this.tradeId = tradeId;
	}

	public String getLegalDocId() {
		return legalDocId;
	}

	public void setLegalDocId(String legalDocId) {
		this.legalDocId = legalDocId;
	}

	public String getCptyId() {
		return cptyId;
	}

	public void setCptyId(String cptyId) {
		this.cptyId = cptyId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cptyId == null) ? 0 : cptyId.hashCode());
		result = prime * result + ((legalDocId == null) ? 0 : legalDocId.hashCode());
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
		CompositeID other = (CompositeID) obj;
		if (cptyId == null) {
			if (other.cptyId != null)
				return false;
		} else if (!cptyId.equals(other.cptyId))
			return false;
		if (legalDocId == null) {
			if (other.legalDocId != null)
				return false;
		} else if (!legalDocId.equals(other.legalDocId))
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
		return "CompositeID [tradeId=" + tradeId + ", legalDocId=" + legalDocId + ", cptyId=" + cptyId + "]";
	}

}
