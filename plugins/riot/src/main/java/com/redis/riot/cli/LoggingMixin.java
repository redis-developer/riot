package com.redis.riot.cli;

import static java.lang.System.setProperty;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.event.Level;
import org.slf4j.simple.SimpleLogger;

import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Spec.Target;

public class LoggingMixin {

	@Spec(Target.MIXEE)
	private CommandSpec mixee;

	Level level = Level.WARN;
	String logFile;
	boolean showDateTime;
	String dateTimeFormat;
	boolean showThreadId;
	boolean showThreadName;
	boolean showLogName;
	boolean showShortLogName;
	boolean levelInBrackets;
	Map<String, Level> logLevels = new LinkedHashMap<>();

	@Option(names = { "-d", "--debug" }, description = "Log in debug mode.")
	public void setDebug(boolean debug) {
		if (debug) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.DEBUG;
		}
	}

	@Option(names = { "-i", "--info" }, description = "Set log level to info.")
	public void setInfo(boolean info) {
		if (info) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.INFO;
		}
	}

//	@Option(names = { "-w", "--warn" }, description = "Set log level to warn.")
	public void setWarn(boolean warn) {
		if (warn) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.WARN;
		}
	}

	@Option(names = { "-q", "--quiet" }, description = "Log errors only.")
	public void setError(boolean error) {
		if (error) {
			getTopLevelCommandLoggingMixin(mixee).level = Level.ERROR;
		}
	}

	@Option(arity = "1..*", names = "--log", description = "Custom log levels (e.g.: io.lettuce=INFO).", paramLabel = "<lvl>")
	public void setLogLevels(Map<String, Level> levels) {
		getTopLevelCommandLoggingMixin(mixee).logLevels = levels;
	}

	@Option(names = "--log-file", description = "Log output target. Can be a path or special values System.out and System.err (default: System.err).", paramLabel = "<file>")
	public void setLogFile(String file) {
		getTopLevelCommandLoggingMixin(mixee).logFile = file;
	}

	@Option(names = "--log-time", description = "Include current date and time in log messages.")
	public void setShowDateTime(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showDateTime = show;
	}

	@Option(names = "--log-time-format", description = "Date and time format to be used in log messages (default: milliseconds since startup).", paramLabel = "<f>")
	public void setDateTimeFormat(String format) {
		getTopLevelCommandLoggingMixin(mixee).dateTimeFormat = format;
	}

	@Option(names = "--log-thread-id", description = "Include current thread ID in log messages.", hidden = true)
	public void setShowThreadId(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showThreadId = show;
	}

	@Option(names = "--log-thread-name", description = "Include current thread name in log messages.", hidden = true)
	public void setShowThreadName(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showThreadName = show;
	}

	@Option(names = "--log-name", description = "Include logger instance name in log messages.", hidden = true)
	public void setShowLogName(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showLogName = show;
	}

	@Option(names = "--log-short", description = "Include last component of logger instance name in log messages.", hidden = true)
	public void setShowShortLogName(boolean show) {
		getTopLevelCommandLoggingMixin(mixee).showShortLogName = show;
	}

	@Option(names = "--log-level", description = "Output log level string in brackets.", hidden = true)
	public void setLevelInBrackets(boolean enable) {
		getTopLevelCommandLoggingMixin(mixee).levelInBrackets = enable;
	}

	public static int executionStrategy(ParseResult parseResult) throws ExecutionException, ParameterException {
		getTopLevelCommandLoggingMixin(parseResult.commandSpec()).configureLogging();
		return ExitCode.OK;
	}

	private static LoggingMixin getTopLevelCommandLoggingMixin(CommandSpec commandSpec) {
		return ((AbstractMainCommand) commandSpec.root().userObject()).loggingMixin;
	}

	public void configureLogging() {
		configureLogging(getTopLevelCommandLoggingMixin(mixee));
	}

	private static void configureLogging(LoggingMixin mixin) {
		setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, mixin.level.name());
		if (mixin.logFile != null) {
			setProperty(SimpleLogger.LOG_FILE_KEY, mixin.logFile);
		}
		setBoolean(SimpleLogger.SHOW_DATE_TIME_KEY, mixin.showDateTime);
		if (mixin.dateTimeFormat != null) {
			setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, mixin.dateTimeFormat);
		}
		setBoolean(SimpleLogger.SHOW_THREAD_ID_KEY, mixin.showThreadId);
		setBoolean(SimpleLogger.SHOW_THREAD_NAME_KEY, mixin.showThreadName);
		setBoolean(SimpleLogger.SHOW_LOG_NAME_KEY, mixin.showLogName);
		setBoolean(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, mixin.showShortLogName);
		setBoolean(SimpleLogger.LEVEL_IN_BRACKETS_KEY, mixin.levelInBrackets);
		setLogLevel("com.amazonaws.internal", Level.ERROR);
		setLogLevel("org.springframework.batch.core.step.builder.FaultTolerantStepBuilder", Level.ERROR);
		setLogLevel("org.springframework.batch.core.step.item.ChunkMonitor", Level.ERROR);
		for (Entry<String, Level> entry : mixin.logLevels.entrySet()) {
			setLogLevel(entry.getKey(), entry.getValue());
		}
	}

	private static void setLogLevel(String key, Level level) {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + key, level.name());
	}

	private static void setBoolean(String property, boolean value) {
		if (value) {
			setProperty(property, String.valueOf(value));
		}
	}

}
