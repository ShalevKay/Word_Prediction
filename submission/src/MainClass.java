import operations.EMROperations;
import software.amazon.awssdk.services.emr.model.StepConfig;

import java.util.LinkedList;
import java.util.List;

public class MainClass {
    public static void main(String[] args) {
        // AWS Configuration
        String region = "us-east-1";
        String s3BucketName = "dsp2";

        EMROperations emrOperations = new EMROperations(region);

        List<StepConfig> steps = new LinkedList<>();
        steps.add(emrOperations.createHadoopStep("Word Counter", "s3://"+s3BucketName+"/jars/WordCounter.jar", "WordCounter", new LinkedList<>()));
        steps.add(emrOperations.createHadoopStep("C1 Step", "s3://"+s3BucketName+"/jars/C1Step.jar", "C1Step", new LinkedList<>()));
        steps.add(emrOperations.createHadoopStep("N1 Step", "s3://"+s3BucketName+"/jars/N1Step.jar", "N1Step", new LinkedList<>()));
        steps.add(emrOperations.createHadoopStep("C2 Step", "s3://"+s3BucketName+"/jars/C2Step.jar", "C2Step", new LinkedList<>()));
        steps.add(emrOperations.createHadoopStep("N2 Step", "s3://"+s3BucketName+"/jars/N2Step.jar", "N2Step", new LinkedList<>()));
        steps.add(emrOperations.createHadoopStep("Final Step", "s3://"+s3BucketName+"/jars/FinalStep.jar", "FinalStep", new LinkedList<>()));

        emrOperations.deployCluster(
                "WordPredictionCluster",
                "s3://"+s3BucketName+"/logs/",
                "m4.large",
                "m4.large",
                3,
                steps,
                "vockey",
                "EMR_DefaultRole",
                "EMR_EC2_DefaultRole",
                "2.9.2"
        );
    }
}