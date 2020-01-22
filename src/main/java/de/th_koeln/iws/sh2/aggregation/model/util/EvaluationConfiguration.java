package de.th_koeln.iws.sh2.aggregation.model.util;

public class EvaluationConfiguration {

	public static class Builder {

		private boolean activityScore, intlScore, ratingScore, citationScore, promScore, sizeScore, affilScore, logScore;

		public Builder useActivityScore() {
			this.activityScore = true;
			return this;
		}

		public Builder useIntlScore() {
			this.intlScore = true;
			return this;
		}

		public Builder useCitationScore() {
			this.citationScore = true;
			return this;
		}

		public Builder usePromScore() {
			this.promScore = true;
			return this;
		}

		public Builder useRatingScore() {
			this.ratingScore = true;
			return this;
		}

		public Builder useSizeScore() {
			this.sizeScore = true;
			return this;
		}

		public Builder useAffilScore() {
			this.affilScore = true;
			return this;
		}

		public Builder useLogScore() {
			this.logScore = true;
			return this;
		}

		public EvaluationConfiguration build() {
			return new EvaluationConfiguration(this);
		}
	}

	private boolean useActivityScore, useIntlScore, useRatingScore, useCitationScore, usePromScore, useSizeScore, useAffilScore, useLogScore;

	private EvaluationConfiguration(Builder builder) {
		this.useActivityScore = builder.activityScore;
		this.useIntlScore = builder.intlScore;
		this.useRatingScore = builder.ratingScore;
		this.useCitationScore = builder.citationScore;
		this.usePromScore = builder.promScore;
		this.useSizeScore = builder.sizeScore;
		this.useAffilScore=builder.affilScore;
		this.useLogScore=builder.logScore;
	}

	public boolean isUseActivityScore() {
		return this.useActivityScore;
	}

	public boolean isUseIntlScore() {
		return this.useIntlScore;
	}

	public boolean isUseCitationScore() {
		return this.useCitationScore;
	}

	public boolean isUsePromScore() {
		return this.usePromScore;
	}

	public boolean isUseRatingScore() {
		return this.useRatingScore;
	}

	public boolean isUseSizeScore() {
		return this.useSizeScore;
	}

	public boolean isUseAffilScore() {
		return this.useAffilScore;
	}

	public boolean isUseLogScore() {
		return this.useLogScore;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("");
		if (this.useCitationScore) builder.append("_citation");
		if (this.useActivityScore) builder.append("_active");
		if (this.useIntlScore) builder.append("_intl");
		if (this.usePromScore) builder.append("_prom");
		if (this.useRatingScore) builder.append("_rating");
		if (this.useSizeScore) builder.append("_size");
		if (this.useAffilScore) builder.append("_affiliation");
		if (this.useLogScore) builder.append("_logs");

		String string = builder.toString();
		if(string.isEmpty()) return "_baseline";
		return builder.toString();
	}
}
