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

        pipeline.apply("ReadFromPostgres", JdbcIO.<Object[]>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "org.postgresql.Driver", "jdbc:postgresql://34.123.211.110:5432/test_db")
                        .withUsername("postgres")
                        .withPassword("rgukt"))
                .withQuery("SELECT id, name FROM user_info")
                .withRowMapper(new JdbcIO.RowMapper<Object[]>() {
                    @Override
                    public Object[] mapRow(ResultSet resultSet) throws SQLException {
                        // Extract id as Integer and name as String
                        return new Object[] {
                            resultSet.getInt("id"),          // 'id' as Integer
                            resultSet.getString("name")      // 'name' as String
                        };
                    }
                })
                .withOutputParallelization(false))
                .apply("WriteToPostgres", JdbcIO.<Object[]>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                "org.postgresql.Driver", "jdbc:postgresql://34.121.69.99:5432/sink_db")
                                .withUsername("postgres")
                                .withPassword("rgukt"))
                        .withStatement("INSERT INTO user_info (id, name) VALUES (?, ?) " +
                                       "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")
                        .withPreparedStatementSetter((JdbcIO.PreparedStatementSetter<Object[]>) (element, query) -> {
                            query.setInt(1, (Integer) element[0]);     // Set 'id' as Integer
                            query.setString(2, (String) element[1]);   // Set 'name' as String
                        }));

        pipeline.run().waitUntilFinish();
    }
}
