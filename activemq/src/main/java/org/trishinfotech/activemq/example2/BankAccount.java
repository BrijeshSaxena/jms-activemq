package org.trishinfotech.activemq.example2;

import java.io.Serializable;

public class BankAccount implements Serializable {
	private static final long serialVersionUID = 8903317706228710187L;
	private long applicationNo;
	private String userName;
	private double depositAmount;
	private long customerId;
	private String atmCardNumber;

	public BankAccount(long applicationNo, String userName, double depositAmount) {
		super();
		this.applicationNo = applicationNo;
		this.userName = userName;
		this.depositAmount = depositAmount;
	}

	public long getApplicationNo() {
		return applicationNo;
	}

	public void setApplicationNo(long applicationNo) {
		this.applicationNo = applicationNo;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public double getDepositAmount() {
		return depositAmount;
	}

	public void setDepositAmount(double depositAmount) {
		this.depositAmount = depositAmount;
	}

	public long getCustomerId() {
		return customerId;
	}

	public void setCustomerId(long customerId) {
		this.customerId = customerId;
	}

	public String getAtmCardNumber() {
		return atmCardNumber;
	}

	public void setAtmCardNumber(String atmCardNumber) {
		this.atmCardNumber = atmCardNumber;
	}

	@Override
	public String toString() {
		return String.format("%10s,  %10s, %10s, %10s, %20s",
				applicationNo, userName, depositAmount, customerId != 0 ? customerId : "NA",
				atmCardNumber != null ? atmCardNumber : "NA");
	}

}
