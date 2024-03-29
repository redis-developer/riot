package com.redis.riot.redis;

import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.item.support.ListItemWriter;

import com.redis.riot.core.ExportProcessorOptions;
import com.redis.riot.core.PredicateItemProcessor;
import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.RiotUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.common.Range;
import com.redis.spring.batch.step.FlushingStepBuilder;
import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.cluster.SlotHash;

public abstract class AbstractReplicationTests extends AbstractTargetTestBase {

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

	protected void execute(Replication replication, TestInfo info) {
		replication.setName(name(info));
		replication.setJobRepository(jobRepository);
		replication.setTransactionManager(transactionManager);
		replication.setRedisClientOptions(redisOptions(getRedisServer()));
		replication.setTargetRedisClientOptions(redisOptions(getTargetRedisServer()));
		replication.run();
	}

	private RedisClientOptions redisOptions(RedisServer redis) {
		RedisClientOptions options = new RedisClientOptions();
		options.setUri(redis.getRedisURI());
		options.setCluster(redis.isRedisCluster());
		return options;
	}

	@Test
	void replication(TestInfo info) throws Throwable {
		generate(info, generator(73));
		Assertions.assertTrue(commands.dbsize() > 0);
		Replication replication = new Replication();
		execute(replication, info);
		Assertions.assertTrue(compare(info).isOk());
	}

	@Test
	void keyProcessor(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		commands.set(key1, value1);
		Replication replication = new Replication();
		replication.setType(ReplicationType.STRUCT);
		replication.setProcessorOptions(processorOptions("#{type.getString()}:#{key}"));
		execute(replication, info);
		Assertions.assertEquals(value1, targetCommands.get("string:" + key1));
	}

	private ExportProcessorOptions processorOptions(String keyExpression) {
		ExportProcessorOptions options = new ExportProcessorOptions();
		options.setKeyExpression(RiotUtils.parseTemplate(keyExpression));
		return options;
	}

	@Test
	void keyProcessorWithDate(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		commands.set(key1, value1);
		Replication replication = new Replication();
		replication.setProcessorOptions(processorOptions(
				String.format("#{#date.parse('%s').getTime()}:#{key}", "2010-05-10T00:00:00.000+0000")));
		execute(replication, info);
		Assertions.assertEquals(value1, targetCommands.get("1273449600000:" + key1));
	}

	@Test
	void filterKeySlot(TestInfo info) throws Exception {
		enableKeyspaceNotifications(client);
		RedisItemReader<String, String, KeyValue<String>> reader = live(structReader(info));
		Range range = Range.to(8000);
		reader.setKeyProcessor(new PredicateItemProcessor<>(k -> range.contains(SlotHash.getSlot(k))));
		ListItemWriter<KeyValue<String>> writer = new ListItemWriter<>();
		generateAsync(info, generator(100));
		FlushingStepBuilder<KeyValue<String>, KeyValue<String>> step = flushingStep(info, reader, writer);
		run(job(info).start(step.build()).build());
		Assertions.assertTrue(writer.getWrittenItems().stream().map(KeyValue::getKey).map(SlotHash::getSlot)
				.allMatch(between(0, 8000)));
	}

	private Predicate<Integer> between(int start, int end) {
		return i -> i >= 0 && i <= end;
	}

}
