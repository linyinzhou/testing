/**
 * Copyright Google Inc. 2016 Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package com.abcd.training.pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface CustomOptions extends PipelineOptions {
	@Description("TradeResult File.")
	@Default.String("TradeResult.csv")
	String getTradeResult();

	void setTradeResult(String input);

	@Description("Trade File.")
	@Default.String("TradeAttribute.csv")
	String getTrade();

	void setTrade(String input);

	@Description("LegalDoc File.")
	@Default.String("LegalDoc.csv")
	String getLegalDoc();

	void setLegalDoc(String input);
}
