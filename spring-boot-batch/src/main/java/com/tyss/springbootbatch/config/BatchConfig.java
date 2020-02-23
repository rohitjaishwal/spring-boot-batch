package com.tyss.springbootbatch.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.Scheduled;

import com.tyss.springbootbatch.dto.Employee;
import com.tyss.springbootbatch.processor.EmployeeItemProcessor;

import lombok.extern.java.Log;

@Configuration
@EnableBatchProcessing
@PropertySource("classpath:application.properties")
@Log
public class BatchConfig {
	@Value("${spring.datasource.driver-class-name}")
	private String driverClassName;
	
	@Value("${spring.datasource.url}")
	private String dbUrl;
	
	@Value("${spring.datasource.username}")
	private String username;
	
	@Value("${spring.datasource.password}")
	private String password;
	
	@Value("${selectAllQuery}")
	private String selectAllQuery;
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private SimpleJobLauncher jobLauncher;

	@Autowired
	public DataSource dataSource;
	
	@Bean
	public DataSource getDataSource() {
		final DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource();
		driverManagerDataSource.setDriverClassName(driverClassName);
		driverManagerDataSource.setUrl(dbUrl);
		driverManagerDataSource.setUsername(username);
		driverManagerDataSource.setPassword(password);
		return driverManagerDataSource;
	}

	@Bean
	public JdbcCursorItemReader<Employee> empCursorItemReader() {
		JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<Employee>();
		reader.setDataSource(dataSource);
		reader.setSql(selectAllQuery);
		reader.setRowMapper(new EmployeeRowMapper());
		return reader;
	}
	
	@Bean
	public ResourcelessTransactionManager resourcelessTransactionManager() {
		return new ResourcelessTransactionManager();
	}

	@Bean
	public MapJobRepositoryFactoryBean mapJobRepositoryFactory(ResourcelessTransactionManager txManager)
			throws Exception {

		MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean(txManager);

		factory.afterPropertiesSet();

		return factory;
	}

	@Bean
	public JobRepository ijobRepository(MapJobRepositoryFactoryBean factory) throws Exception {
		return factory.getObject();
	}

	@Scheduled(cron = "*/60 * * * * *")
	public void perform() throws Exception {

		log.info("Job Started at :" + new java.util.Date());

		JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();

		JobExecution execution = jobLauncher.run(employeeExportJob(), param);

		log.info("Job finished with status :" + execution.getStatus());
	}


	public class EmployeeRowMapper implements RowMapper<Employee> {

		@Override
		public Employee mapRow(ResultSet resultSet, int rowNum) throws SQLException {
			Employee employee = new Employee();
			employee.setEmpid(resultSet.getInt("empid"));
			employee.setName(resultSet.getString("name"));
			employee.setBirthDay(resultSet.getDate("birthDay"));
			employee.setJoiningDay(resultSet.getDate("joiningDay"));
			employee.setEmail(resultSet.getString("email"));
			return employee;
		}
	}

	@Bean
	public EmployeeItemProcessor getEmployeeItemProcessor() {
		return new EmployeeItemProcessor();
	}

	@Bean
	public FlatFileItemWriter<Employee> writer() {
		FlatFileItemWriter<Employee> employeeWriter = new FlatFileItemWriter<Employee>();
		employeeWriter.setResource(new ClassPathResource("employees.csv"));
		employeeWriter.setLineAggregator(new DelimitedLineAggregator<Employee>() {
			{
				setDelimiter(",");
				setFieldExtractor(new BeanWrapperFieldExtractor<Employee>() {
					{
						setNames(new String[] { "empid", "name", "birthDay", "joiningDay", "email" });
					}
				});
			}
		});
		return employeeWriter;
	}

	@Bean
	public Step step() {
		return stepBuilderFactory.get("step").<Employee, Employee>chunk(10).reader(empCursorItemReader())
				.processor(getEmployeeItemProcessor()).writer(writer()).build();
	}

	@Bean
	public Job employeeExportJob() {
		return jobBuilderFactory.get("employeeExportJob").incrementer(new RunIdIncrementer()).flow(step()).end()
				.build();
	}
	
	@Bean
	public SimpleJobLauncher simpleJobLauncher(JobRepository jobRepository) {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		return launcher;
	}
}
