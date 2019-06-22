package com.redislabs.riot.cli.file;

import java.io.File;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import com.redislabs.riot.cli.AbstractWriterCommand;

import lombok.Getter;
import picocli.CommandLine.Option;

public abstract class AbstractFileWriterCommand extends AbstractWriterCommand {

	@Option(names = "--file", required = true, description = "Path to output file.", paramLabel = "<path>")
	private File file;
	@Getter
	@Option(names = "--append", description = "Append to the file if it already exists.")
	private boolean append;
	@Getter
	@Option(names = "--encoding", description = "Encoding for the output file.", paramLabel = "<charset>")
	private String encoding = FlatFileItemWriter.DEFAULT_CHARSET;
	@Getter
	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush.")
	private boolean forceSync;

	protected Resource resource() {
		return new FileSystemResource(file);
	}

	@Override
	protected String getTargetDescription() {
		return "file " + file;
	}

}