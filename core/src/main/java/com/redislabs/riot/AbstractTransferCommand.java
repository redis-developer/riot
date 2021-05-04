package com.redislabs.riot;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.builder.StepBuilder;
import picocli.CommandLine;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractTransferCommand extends AbstractTaskCommand {

    @CommandLine.Mixin
    private TransferOptions transferOptions = TransferOptions.builder().build();

    protected <I, O> RiotStepBuilder<I, O> riotStep(StepBuilder stepBuilder, String taskName) {
        return new RiotStepBuilder<I, O>(stepBuilder, transferOptions).taskName(taskName);
    }

}