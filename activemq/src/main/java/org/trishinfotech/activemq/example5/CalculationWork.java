package org.trishinfotech.activemq.example5;

import java.io.Serializable;

public class CalculationWork implements Serializable {

	private static final String COMMA_STR = ",";
	private static final long serialVersionUID = 710578808598511209L;
	protected String calculateName;
	protected String value;

	public CalculationWork(String calculateName, String value) {
		super();
		this.calculateName = calculateName;
		this.value = value;
	}

	public String getCalculateName() {
		return calculateName;
	}

	public void setCalculateName(String calculateName) {
		this.calculateName = calculateName;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[calculateName=").append(calculateName).append(", value=").append(value)
				.append("]");
		return builder.toString();
	}

	public static CalculationWork valueOf(String line) {
		CalculationWork calculationWork = null;
		if (line != null) {
			String[] words = line.split(COMMA_STR);
			if (words.length >= 2) {
				calculationWork = new CalculationWork(words[0].trim(), words[1].trim());
			}
		}
		return calculationWork;
	}

}
