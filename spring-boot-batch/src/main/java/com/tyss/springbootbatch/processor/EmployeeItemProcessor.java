package com.tyss.springbootbatch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.tyss.springbootbatch.dto.Employee;

public class EmployeeItemProcessor implements ItemProcessor<Employee, Employee> {

	@Override
	public Employee process(Employee employee) throws Exception {
		return employee;
	}
	
}
