package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class App {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

        pipeline.apply("ReadFromPostgres", JdbcIO.<String[]>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "org.postgresql.Driver", "jdbc:postgresql://34.123.211.110:5432/test_db")
                        .withUsername("postgres")
                        .withPassword("rgukt"))
                .withQuery("SELECT id, name FROM user1") // Query to fetch all necessary columns
                .withRowMapper(new JdbcIO.RowMapper<String[]>() {
                    @Override
                    public String[] mapRow(ResultSet resultSet) throws SQLException {
                        // Extract both columns from the result set
                        return new String[] {
                            resultSet.getString("id"),
                            resultSet.getString("name")
                        };
                    }
                })
                .withOutputParallelization(false))
                .apply("WriteToPostgres", JdbcIO.<String[]>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                "org.postgresql.Driver", "jdbc:postgresql://34.121.69.99:5432/sink_db")
                                .withUsername("postgres")
                                .withPassword("rgukt"))
                        .withStatement("INSERT INTO user1 (id, name) VALUES (?, ?)")
                        .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<String[]>) (element, query) -> {
                            query.setString(1, element[0]); // Set 'id'
                            query.setString(2, element[1]); // Set 'name'
                        }));

        pipeline.run().waitUntilFinish();
    }
}
