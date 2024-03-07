package bootiful.batch.worker;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootApplication
class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }

}

@Configuration
class RabbitConfiguration {

    static final String REQUESTS = "requests";

    static final String REPLIES = "replies";

}

@Configuration
class WorkerConfiguration {

    static final String TASKLET = "tasklet";

    private final RemotePartitioningWorkerStepBuilder workerStepBuilderFactory;


    WorkerConfiguration(BeanFactory beanFactory , JobRepository repository, JobExplorer jobExplorer) {
        this.workerStepBuilderFactory = new RemotePartitioningWorkerStepBuilder("worker-step", repository)
                .jobExplorer(jobExplorer)
                .beanFactory(beanFactory);

    }

    @Bean
    DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    IntegrationFlow inboundFlow(ConnectionFactory connectionFactory) {
        return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory, RabbitConfiguration.REQUESTS))
                .channel(requests())
                .get();
    }


    @Bean
    DirectChannel replies() {
        return new DirectChannel();
    }

    @Bean
    IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow
                .from(replies())
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(RabbitConfiguration.REPLIES))
                .get();
    }


    @Bean
    Step workerStep(PlatformTransactionManager transactionManager, @Qualifier(TASKLET) Tasklet tasklet) {

        return this.workerStepBuilderFactory
                .inputChannel(requests())
                .outputChannel(replies())
                .tasklet(tasklet, transactionManager)
                .build();
    }

    @Bean(TASKLET)
    @StepScope
    Tasklet tasklet(@Value("#{stepExecutionContext['partition']}") String partition) {
        return (contribution, chunkContext) -> {
            System.out.println("processing " + partition);
            return RepeatStatus.FINISHED;
        };
    }

}