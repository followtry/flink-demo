package app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * 流式表的窗口 Sql
 * 在 flink1.9.2版本中还不支持Watermark 操作
 * @author jingzhongzhi
 * @since 2020/2/9
 */
public class StreamWindowSql {
    /**
     * main.
     */
    public static void main(String[] args) throws Exception {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // use blink planner in streaming mode,
        // because watermark statement is only available in blink planner.
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
    
        // write source data into temporary file and get the absolute path
        String contents = "1,beer,3,2019-12-12 00:00:01\n" +
                "1,diaper,4,2019-12-12 00:00:02\n" +
                "2,pen,3,2019-12-12 00:00:04\n" +
                "2,rubber,3,2019-12-12 00:00:06\n" +
                "3,rubber,2,2019-12-12 00:00:05\n" +
                "4,beer,1,2019-12-12 00:00:08";
        String path = createTempFile(contents);
    
        // register table via DDL with watermark,
        // the events are out of order, hence, we use 3 seconds to wait the late events
        String ddl = "CREATE TABLE orders (\n" +
                "  user_id INT,\n" +
                "  product STRING,\n" +
                "  amount INT,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  watermark for ts AS ts - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',\n" +
                "  'connector.path' = '" + path + "',\n" +
                "  'format.type' = 'csv'\n" +
                ")";
        tEnv.sqlUpdate(ddl);
    
        // run a SQL query on the table and retrieve the result as a new Table
        String query = "SELECT\n" +
                "  CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start,\n" +
                "  COUNT(*) order_num,\n" +
                "  SUM(amount) total_amount,\n" +
                "  COUNT(DISTINCT product) unique_products\n" +
                "FROM orders\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";
        Table result = tEnv.sqlQuery(query);
        tEnv.toAppendStream(result, Row.class).print();
    
        // submit the job
        tEnv.execute("Streaming Window SQL Job");
    
        // should output:
        // 2019-12-12 00:00:00.000,3,10,3
        // 2019-12-12 00:00:05.000,3,6,2
    }

    /**
     * Creates a temporary file with the contents and returns the absolute path.
     */
    private static String createTempFile(String contents) throws IOException {
            File tempFile = File.createTempFile("orders", ".csv");
            tempFile.deleteOnExit();
            FileUtils.writeFileUtf8(tempFile, contents);
            return tempFile.toURI().toString();
    }
}
