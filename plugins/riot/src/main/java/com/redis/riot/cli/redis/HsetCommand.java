package com.redis.riot.cli.redis;

import com.redis.riot.core.operation.HsetBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "hset", aliases = "hmset", description = "Set hashes from input")
public class HsetCommand extends AbstractRedisCommand {

	@Mixin
	private FieldFilteringArgs filteringArgs = new FieldFilteringArgs();

	@Override
	protected HsetBuilder operationBuilder() {
		HsetBuilder builder = new HsetBuilder();
		filteringArgs.configure(builder);
		return builder;
	}

}