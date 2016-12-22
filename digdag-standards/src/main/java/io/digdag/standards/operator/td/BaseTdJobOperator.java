package io.digdag.standards.operator.td;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.treasuredata.client.TDClientException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import io.digdag.util.ConfigSelector;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

abstract class BaseTdJobOperator
        extends BaseOperator
{
    private static final String DONE_JOB_ID = "doneJobId";

    protected final TaskState state;
    protected final Config params;
    private final Map<String, String> env;

    protected final DurationInterval pollInterval;
    protected final DurationInterval retryInterval;

    BaseTdJobOperator(OperatorContext context, Map<String, String> env, Config systemConfig)
    {
        super(context);

        this.params = request.getConfig().mergeDefault(
                request.getConfig().getNestedOrGetEmpty("td"));

        this.state = TaskState.of(request);
        this.env = env;

        this.pollInterval = TDOperator.pollInterval(systemConfig);
        this.retryInterval = TDOperator.retryInterval(systemConfig);
    }

    static ConfigSelector.Builder configSelectorBuilder()
    {
        return ConfigSelector.builderOfScope("td")
            .addSecretAccess("use_ssl", "proxy.enabled", "proxy.host", "proxy.port", "proxy.user", "proxy.password", "proxy.use_ssl", "endpoint", "host", "port", "user", "database")
            .addSecretOnlyAccess("apikey")
            ;
    }

    @Override
    public final TaskResult runTask()
    {
        try (TDOperator op = TDOperator.fromConfig(env, params, context.getSecrets().getSecrets("td"))) {

            Optional<String> doneJobId = state.params().getOptional(DONE_JOB_ID, String.class);
            TDJobOperator job;
            if (!doneJobId.isPresent()) {
                job = op.runJob(state, "job", pollInterval, retryInterval, (jobOperator, domainKey) -> startJob(jobOperator, domainKey));
                state.params().set(DONE_JOB_ID, job.getJobId());
            }
            else {
                job = op.newJobOperator(doneJobId.get());
            }

            // Get the job results
            TaskResult taskResult = processJobResult(op, job);

            // Set last_job_id param
            taskResult.getStoreParams()
                    .getNestedOrSetEmpty("td")
                    .set("last_job_id", job.getJobId());

            return taskResult;
        }
        catch (TDClientException ex) {
            throw propagateTDClientException(ex);
        }
    }

    protected static TaskExecutionException propagateTDClientException(TDClientException ex)
    {
        return new TaskExecutionException(ex);
    }

    protected abstract String startJob(TDOperator op, String domainKey);

    protected TaskResult processJobResult(TDOperator op, TDJobOperator job)
    {
        return TaskResult.empty(request);
    }
}
