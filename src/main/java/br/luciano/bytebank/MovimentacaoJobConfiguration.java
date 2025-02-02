package br.luciano.bytebank;

import br.luciano.bytebank.mappers.MovimentacaoMapper;
import br.luciano.bytebank.models.Movimentacao;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class MovimentacaoJobConfiguration {
    @Autowired
    PlatformTransactionManager transactionManager;

    @Bean
    public Job jobMovimentacao(Step step1, JobRepository jobRepository) {
        return new JobBuilder("importacao-movimentacao", jobRepository)
                .start(step1)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    public Step step1(ItemReader<Movimentacao> movimentacaoReader, ItemWriter<Movimentacao> movimentacaoWriter, JobRepository jobRepository) {
        return new StepBuilder("step-inicial", jobRepository)
                .<Movimentacao, Movimentacao>chunk(200, transactionManager)
                .reader(movimentacaoReader)
                .writer(movimentacaoWriter)
                .build();
    }

    @Bean
    public ItemReader<Movimentacao> movimentacaoReader() {
        return new FlatFileItemReaderBuilder<Movimentacao>()
                .name("bank-movimentacao-csv")
                .resource(new FileSystemResource("files/dados_ficticios.csv"))
                .comments("Nome")
                .delimited()
                .delimiter("|")
                .names("nome", "cpf", "agencia", "conta", "valor", "mesReferencia")
                .fieldSetMapper(new MovimentacaoMapper())
                .build();
    }

    @Bean
    public ItemWriter<Movimentacao> movimentacaoWriter(DataSource dataSource) {
        String sql = "insert into tb_movimentacao (nome, cpf, agencia, conta, valor, mes_referencia, hora_importacao) " +
                " values (:nome, :cpf, :agencia, :conta, :valor, :mesReferencia, :horaImportacao)";

        return new JdbcBatchItemWriterBuilder<Movimentacao>()
                .dataSource(dataSource)
                .sql(sql)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }
}
