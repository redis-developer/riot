package com.redis.riot.core;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.spring.batch.util.KeyComparisonItemReader;
import com.redis.testcontainers.RedisServer;

public abstract class ReplicationTests extends AbstractTargetTestBase {

    public static final String BEERS_JSON_URL = "https://storage.googleapis.com/jrx/beers.json";

    public static final int BEER_CSV_COUNT = 2410;

    public static final int BEER_JSON_COUNT = 216;

    protected static String name(Map<String, String> beer) {
        return beer.get("name");
    }

    protected static String style(Map<String, String> beer) {
        return beer.get("style");
    }

    protected static double abv(Map<String, String> beer) {
        return Double.parseDouble(beer.get("abv"));
    }

    private PrintWriter printWriter = new PrintWriter(System.out);

    private ReplicationExecutionContext replicationContext;

    @BeforeAll
    public void setupReplicationContext() {
        this.replicationContext = new ReplicationExecutionContext(clientOptions(getRedisServer()),
                clientOptions(getTargetRedisServer()));
    }

    private RedisOptions clientOptions(RedisServer server) {
        RedisOptions options = new RedisOptions();
        options.setCluster(server.isCluster());
        RedisUriOptions uriOptions = new RedisUriOptions();
        uriOptions.setUri(server.getRedisURI());
        options.setUriOptions(uriOptions);
        return options;
    }

    @AfterAll
    public void teardownReplicationContext() {
        this.replicationContext.close();
    }

    protected void execute(Replication executable, TestInfo info) {
        executable.setName(name(info));
        executable.execute(replicationContext);
    }

    @Test
    void replicate(TestInfo info) throws Throwable {
        generate(info);
        Assertions.assertTrue(commands.dbsize() > 0);
        Replication replicate = new Replication(printWriter);
        replicate.execute(replicationContext);
        Assertions.assertTrue(compare(info));
    }

    @Test
    void keyProcessor(TestInfo info) throws Throwable {
        String key1 = "key1";
        String value1 = "value1";
        commands.set(key1, value1);
        Replication replication = new Replication(printWriter);
        replication.setProcessorOptions(operatorOptions("#{type}:#{key}"));
        execute(replication, info);
        Assertions.assertEquals(value1, targetCommands.get("string:" + key1));
    }

    private KeyValueProcessorOptions operatorOptions(String keyExpression) {
        KeyValueProcessorOptions operatorOptions = new KeyValueProcessorOptions();
        operatorOptions.setKeyExpression(SpelUtils.parseTemplate(keyExpression));
        return operatorOptions;
    }

    @Test
    void keyProcessorWithDate(TestInfo info) throws Throwable {
        String key1 = "key1";
        String value1 = "value1";
        commands.set(key1, value1);
        Replication replication = new Replication(printWriter);
        replication.setProcessorOptions(
                operatorOptions(String.format("#{#date.parse('%s').getTime()}:#{key}", "2010-05-10T00:00:00.000+0000")));
        execute(replication, info);
        Assertions.assertEquals(value1, targetCommands.get("1273449600000:" + key1));
    }

    protected KeyComparisonItemReader comparisonReader(TestInfo info) {
        KeyComparisonItemReader comparator = new KeyComparisonItemReader(structReader(info, client),
                structReader(info, targetClient).keyValueProcessor());
        comparator.setTtlTolerance(Duration.ofMillis(100));
        return comparator;
    }

}