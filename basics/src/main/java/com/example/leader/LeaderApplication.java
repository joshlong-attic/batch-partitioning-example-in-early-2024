package com.example.leader;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Date;

@SpringBootApplication
public class LeaderApplication {

    @Bean
    ApplicationRunner applicationRunner(JobLauncher jobLauncher, @Qualifier(JOB_1) Job job) {
        return args -> {
            var execution = jobLauncher
                    .run(job, new JobParametersBuilder()
                            .addDate("start", new Date())
                            .toJobParameters());

            System.out.println(execution.getExitStatus().toString());
        };
    }

    // job
    //    0-N steps || tasklet
    //			 chunk (for N records)
    //              itemReader | itemProcessor | itemWriter
    //


    record Customer(Integer id, String email) {
    }

    static final String JOB_1 = "job1";


//    @Bean
//    @BatchDataSource
//    DataSource batchAdminDataSource (){ ... }

    static final String STEPS_1 = "step1";

    @Bean(STEPS_1)
    Step step1(JobRepository repository, DataSource dataSource, PlatformTransactionManager transactionManager,
               @Value("file://${HOME}/Desktop/data.csv") Resource dataCsvFile) {

        var stepBuilder = new StepBuilder("step1", repository);

        var fileItemReader = new FlatFileItemReaderBuilder<Customer>()
                .name("dataCsvReader")
                .resource(dataCsvFile)
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new Customer(fieldSet.readInt(0), fieldSet.readString(1)))
                .delimited()
                .delimiter(",")
                .names("id", "email")
                .build();

        var jdbcItemWriter = new JdbcBatchItemWriterBuilder<Customer>()
                .sql("insert into customer (id, name) values(?,?)")
                .beanMapped()
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setInt(1, item.id());
                    ps.setString(2, item.email());
                })
                .dataSource(dataSource)
                .build();

        return stepBuilder
                .<Customer, Customer>chunk(3, transactionManager)
                .reader(fileItemReader)
                .writer(jdbcItemWriter)
                .build();
    }

    static final String STEPS_SETUP = "setupStep";

    @Bean(STEPS_SETUP)
    Step setup(PlatformTransactionManager transactionManager, JdbcClient jdbcClient, JobRepository repository) {
        var stepBuilder = new StepBuilder("step1", repository);
        return stepBuilder
                .tasklet((contribution, chunkContext) -> {
                    System.out.println("setup some ambient state...");
                    jdbcClient.sql("delete from customer").update();
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean(JOB_1)
    Job job(JobRepository repository, @Qualifier(STEPS_SETUP) Step setup, @Qualifier(STEPS_1) Step s1) {
        return new JobBuilder("my-job", repository)
                .start(setup)
                .next(s1)
                .build();
    }

    public static void main(String[] args) {
        SpringApplication.run(LeaderApplication.class, args);
    }

}
