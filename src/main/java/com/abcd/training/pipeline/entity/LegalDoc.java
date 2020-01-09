package com.abcd.training.pipeline.entity;

import java.io.Serializable;

public class LegalDoc implements Serializable {

	private static final long serialVersionUID = -4656677248150175281L;

	public LegalDoc(String legalDocId, String cptyId, String nettable, String collateralizable,
			String collateralBalance) {
		super();
		this.legalDocId = legalDocId;
		this.cptyId = cptyId;
		this.nettable = nettable;
		this.collateralizable = collateralizable;
		this.collateralBalance = collateralBalance;
	}

	private String legalDocId;

	private String cptyId;

	private String nettable;

	private String collateralizable;

	private String collateralBalance;

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

	public String getNettable() {
		return nettable;
	}

	public void setNettable(String nettable) {
		this.nettable = nettable;
	}

	public String getCollateralizable() {
		return collateralizable;
	}

	public void setCollateralizable(String collateralizable) {
		this.collateralizable = collateralizable;
	}

	public String getCollateralBalance() {
		return collateralBalance;
	}

	public void setCollateralBalance(String collateralBalance) {
		this.collateralBalance = collateralBalance;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((collateralBalance == null) ? 0 : collateralBalance.hashCode());
		result = prime * result + ((collateralizable == null) ? 0 : collateralizable.hashCode());
		result = prime * result + ((cptyId == null) ? 0 : cptyId.hashCode());
		result = prime * result + ((legalDocId == null) ? 0 : legalDocId.hashCode());
		result = prime * result + ((nettable == null) ? 0 : nettable.hashCode());
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
		LegalDoc other = (LegalDoc) obj;
		if (collateralBalance == null) {
			if (other.collateralBalance != null)
				return false;
		} else if (!collateralBalance.equals(other.collateralBalance))
			return false;
		if (collateralizable == null) {
			if (other.collateralizable != null)
				return false;
		} else if (!collateralizable.equals(other.collateralizable))
			return false;
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
		if (nettable == null) {
			if (other.nettable != null)
				return false;
		} else if (!nettable.equals(other.nettable))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "LegalDoc [legalDocId=" + legalDocId + ", cptyId=" + cptyId + ", nettable=" + nettable
				+ ", collateralizable=" + collateralizable + ", collateralBalance=" + collateralBalance + "]";
	}

}
