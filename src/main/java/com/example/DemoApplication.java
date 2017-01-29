package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Map;

@SpringBootApplication
@EnableBatchProcessing
public class DemoApplication {

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private TaskletA taskletA;

    @Autowired
    private TaskletB taskletB;

    @Bean
    public JobLauncher jobLauncher() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();

        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(taskExecutor);

        return jobLauncher;
    }

    @Component
    public class TaskletA implements Tasklet {

        private final Logger logger = LoggerFactory.getLogger(this.getClass());

        @Override
        public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

            JobExecution jobExecution = chunkContext.getStepContext().getStepExecution().getJobExecution();
            String message = (String) jobExecution.getJobParameters().getParameters().get("id").getValue();

            logger.info("User-ID: " + message);

            return RepeatStatus.FINISHED;
        }
    }

    @Component
    public class TaskletB implements Tasklet {

        private final Logger logger = LoggerFactory.getLogger(this.getClass());

        @Override
        public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

            JobExecution jobExecution = chunkContext.getStepContext().getStepExecution().getJobExecution();
            String message = (String) jobExecution.getJobParameters().getParameters().get("id").getValue();

            logger.info("User-ID: " + message);

            return RepeatStatus.FINISHED;
        }
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Configuration
    public class config {

        // STEPS -----------------------------------------

        @Bean
        public Step taskletAStep() {
            return stepBuilderFactory.get("taskletA")
                    .tasklet(taskletA)
                    .build();
        }

        @Bean
        public Step taskletBStep() {
            return stepBuilderFactory.get("taskletB")
                    .tasklet(taskletB)
                    .build();
        }

        // JOBS ------------------------------------------

        @Bean
        public Job sampleJob() throws Exception {
            return jobBuilderFactory.get("sampleJob")
                    .start(taskletAStep())
                    .next(taskletBStep())
                    .build();
        }
    }

	@RestController
    @RequestMapping("/jobs")
    public class SampleController {

        @Autowired
        private Job sampleJob;

        @Autowired
        private JobOperator jobOperator;

	    @RequestMapping(value = "/create", method = RequestMethod.GET)
        public Long createJob() {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addDate("date", new Date())
                    .addString("id", "testing 123")
                    .toJobParameters();

            try {
                JobExecution jobExecution = jobLauncher().run(sampleJob, jobParameters);
                logger.info("JOB EXECUTION ID: " + jobExecution.getId());
                return jobExecution.getId();
            } catch (Exception e) {
                logger.error(String.format("Not able to create UserValidation Batch Job: %s", e.getMessage()));
            }
            return null;
	    }

        @RequestMapping(value = "/{jobId}", method = RequestMethod.GET)
        public ResponseEntity<?> getJobDetails(@PathVariable Long jobId) throws NoSuchJobExecutionException {

            Map<Long, String> summary = jobOperator.getStepExecutionSummaries(jobId);

            return new ResponseEntity<>(summary, HttpStatus.OK);
        }
    }
}