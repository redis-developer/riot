package com.redis.riot;

import org.springframework.core.NestedExceptionUtils;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class OneLineLogFormat extends Formatter {

    @Override
    public String format(LogRecord logRecord) {
        String message = formatMessage(logRecord);
        if (logRecord.getThrown() != null) {
            Throwable rootCause = NestedExceptionUtils.getRootCause(logRecord.getThrown());
            if (rootCause != null && rootCause.getMessage() != null) {
                return String.format("%s: %s%n", message, rootCause.getMessage());
            }
        }
        return String.format("%s%n", message);
    }

}