package com.redislabs.recharge.redis.list;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.redis.CollectionRedisWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class LPushWriter extends CollectionRedisWriter<ListConfiguration> {

	public LPushWriter(ListConfiguration config, GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		super(config, pool);
	}

	@Override
	protected RedisFuture<?> write(String key, String member, Map record,
			RediSearchAsyncCommands<String, String> commands) {
		return commands.lpush(key, member);
	}

}