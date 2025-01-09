package operations;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.List;

public class EMROperations {

    private final EmrClient emrClient;

    /**
     * Constructor: Initialize the EMR client with region
     *
     * @param region    AWS Region (e.g., "us-east-1")
     */
    public EMROperations(String region) {
        this.emrClient = EmrClient.builder()
                .region(Region.of(region))
                .build();
    }

    /**
     * Creates a Hadoop step configuration for the EMR cluster.
     *
     * @param stepName Name of the step
     * @param jarPath  S3 path to the MapReduce JAR file
     * @param mainClass Main class for the job
     * @param args     Arguments for the job (e.g., input/output paths)
     * @return StepConfig
     */
    public StepConfig createHadoopStep(String stepName, String jarPath, String mainClass, List<String> args) {
        // with logger
        HadoopJarStepConfig hadoopJarStep = HadoopJarStepConfig.builder()
                .jar(jarPath)
                .mainClass(mainClass)
                .args(args)
                .build();

        return StepConfig.builder()
                .name(stepName)
                .hadoopJarStep(hadoopJarStep)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
    }

    /**
     * Deploys an EMR cluster with the given configuration.
     *
     * @param clusterName    Name of the EMR cluster
     * @param logUri         S3 path for EMR logs
     * @param masterInstance Instance type for the master node
     * @param slaveInstance  Instance type for slave nodes
     * @param instanceCount  Number of instances (total)
     * @param steps          List of StepConfig
     * @param ec2KeyName     EC2 key pair name for SSH access
     * @param serviceRole    IAM Service Role for EMR
     * @param jobFlowRole    IAM Job Flow Role for EMR EC2 instances
     * @return Cluster ID of the newly created EMR cluster
     */
    public String deployCluster(String clusterName, String logUri, String masterInstance,
                                String slaveInstance, int instanceCount, List<StepConfig> steps,
                                String ec2KeyName, String serviceRole, String jobFlowRole, String hadoopVersion) {

        JobFlowInstancesConfig instancesConfig = JobFlowInstancesConfig.builder()
                .ec2KeyName(ec2KeyName)
                .instanceCount(instanceCount)
                .masterInstanceType(masterInstance)
                .slaveInstanceType(slaveInstance)
                .keepJobFlowAliveWhenNoSteps(false)
                .hadoopVersion(hadoopVersion)
                .build();

        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name(clusterName)
                .logUri(logUri)
                .serviceRole(serviceRole)
                .jobFlowRole(jobFlowRole)
                .instances(instancesConfig)
                .steps(steps)
                .releaseLabel("emr-5.11.0") // EMR version
                .visibleToAllUsers(true)
                .build();

        RunJobFlowResponse response = emrClient.runJobFlow(request);
        System.out.println("EMR cluster created: " + response.jobFlowId());
        return response.jobFlowId();
    }

    /**
     * Closes the EMR client
     */
    public void close() {
        this.emrClient.close();
    }
}
