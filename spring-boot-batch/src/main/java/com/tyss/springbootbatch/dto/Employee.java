package com.tyss.springbootbatch.dto;

import java.sql.Date;

public class Employee {
	private String name;
	private Integer empid;
	private Date birthDay;
	private Date joiningDay;
	private String email;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getEmpid() {
		return empid;
	}

	public void setEmpid(Integer empid) {
		this.empid = empid;
	}

	public Date getBirthDay() {
		return birthDay;
	}

	public void setBirthDay(Date birthDay) {
		this.birthDay = birthDay;
	}

	public Date getJoiningDay() {
		return joiningDay;
	}

	public void setJoiningDay(Date joiningDay) {
		this.joiningDay = joiningDay;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
}
