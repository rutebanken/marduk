
package no.rutebanken.marduk.routes.otp.otp1;

import io.fabric8.kubernetes.api.model.EnvVar;
import no.rutebanken.marduk.routes.otp.AbstractKubernetesJobRunner;
import no.rutebanken.marduk.routes.otp.remote.OtpGraphBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


/**
 * Build the OTP graph in a standalone pod and wait until the pod terminates.
 * The pod is created by a Kubernetes job.
 * A Kubernetes CronJob is used as a template for the job.
 */
@Component
public class BaseGraphBuilder extends AbstractKubernetesJobRunner implements OtpGraphBuilder {

    private static final String OTP_GCS_WORK_DIR_ENV_VAR = "OTP_GCS_WORK_DIR";
    private static final String OTP_GCS_BASE_GRAPH_DIR_ENV_VAR = "OTP_GCS_BASE_GRAPH_DIR";
    private static final String OTP_SKIP_TRANSIT_ENV_VAR = "OTP_SKIP_TRANSIT";

    @Value("${otp.graph.build.remote.kubernetes.cronjob:graph-builder}")
    private String graphBuilderCronJobName;

    @Override
    public String getCronJobName() {
        return graphBuilderCronJobName;
    }

    protected List<EnvVar> getEnvVars(String otpWorkDir) {
        List<EnvVar> envVars = new ArrayList<>();
        envVars.add(new EnvVar(OTP_GCS_WORK_DIR_ENV_VAR, otpWorkDir, null));
        envVars.add(new EnvVar(OTP_SKIP_TRANSIT_ENV_VAR, "--skipTransit", null));
        envVars.add(new EnvVar(OTP_GCS_BASE_GRAPH_DIR_ENV_VAR, otpWorkDir, null));
        return  envVars;

    }

    @Override
    public void build(String otpWorkDir, String timestamp) {
            runJob(getEnvVars(otpWorkDir), timestamp);
    }
}