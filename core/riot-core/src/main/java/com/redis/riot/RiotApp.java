package com.redis.riot;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import io.lettuce.core.RedisURI;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParseResult;

@Command(sortOptions = false, versionProvider = ManifestVersionProvider.class, subcommands = GenerateCompletionCommand.class, abbreviateSynopsis = true)
public class RiotApp extends HelpCommand {

	private static final String ROOT_LOGGER = "";

	@Option(names = { "-V", "--version" }, versionHelp = true, description = "Print version information and exit.")
	private boolean versionRequested;
	@ArgGroup(heading = "Redis connection options%n", exclusive = false)
	private RedisOptions redisOptions = new RedisOptions();
	@Mixin
	private LoggingOptions loggingOptions = new LoggingOptions();

	public RedisOptions getRedisOptions() {
		return redisOptions;
	}

	public LoggingOptions getLoggingOptions() {
		return loggingOptions;
	}

	private int executionStrategy(ParseResult parseResult) {
		configureLogging();
		return new CommandLine.RunLast().execute(parseResult); // default execution strategy
	}

	private int executionStragegyRunFirst(ParseResult parseResult) {
		configureLogging();
		return new CommandLine.RunFirst().execute(parseResult);
	}

	protected void configureLogging() {
		InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
		LogManager.getLogManager().reset();
		Logger activeLogger = Logger.getLogger(ROOT_LOGGER);
		ConsoleHandler handler = new ConsoleHandler();
		handler.setLevel(Level.ALL);
		handler.setFormatter(loggingOptions.isStacktrace() ? new StackTraceOneLineLogFormat() : new OneLineLogFormat());
		activeLogger.addHandler(handler);
		Logger.getLogger(ROOT_LOGGER).setLevel(loggingOptions.getLevel());
		Logger.getLogger("com.redis").setLevel(loggingOptions.getRiotLevel());
	}

	public int execute(String... args) {
		return commandLine().execute(args);
	}

	public RiotCommandLine commandLine() {
		RiotCommandLine commandLine = new RiotCommandLine(this, this::executionStragegyRunFirst);
		commandLine.setExecutionStrategy(this::executionStrategy);
		commandLine.setExecutionExceptionHandler(this::handleExecutionException);
		registerConverters(commandLine);
		commandLine.setCaseInsensitiveEnumValuesAllowed(true);
		commandLine.setUnmatchedOptionsAllowedAsOptionParameters(false);
		return commandLine;
	}

	private int handleExecutionException(Exception ex, CommandLine cmd, ParseResult parseResult) {
		// bold red error message
		cmd.getErr().println(cmd.getColorScheme().errorText(ex.getMessage()));
		return cmd.getExitCodeExceptionMapper() != null ? cmd.getExitCodeExceptionMapper().getExitCode(ex)
				: cmd.getCommandSpec().exitCodeOnExecutionException();
	}

	protected void registerConverters(CommandLine commandLine) {
		commandLine.registerConverter(RedisURI.class, RedisURI::create);
		SpelExpressionParser parser = new SpelExpressionParser();
		commandLine.registerConverter(Expression.class, parser::parseExpression);
	}

}