import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class QDRM_Construct_AI_PipelineMonitor {

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String GRPC_SERVER = "localhost:50051";

    private KafkaStreams kafkaStreams;
    private ManagedChannel channel;
    private ListeningExecutorService executorService;
    private ExecutorService asyncExecutor;

    public QDRM_Construct_AI_PipelineMonitor() {
        this.executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(5));
        this.asyncExecutor = Executors.newFixedThreadPool(5);
    }

    public void init() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("pipeline_monitor_topic");

        stream.foreach((key, value) -> {
            sendToGRPCServer(key, value);
        });

        this.kafkaStreams = new KafkaStreams(builder.build(), getKafkaConfig());
        this.kafkaStreams.start();

        this.channel = ManagedChannelBuilder.forAddress(GRPC_SERVER, 50051).usePlaintext().build();
    }

    private void sendToGRPCServer(String key, String value) {
        PipelineMonitorServiceGrpc.PipelineMonitorServiceStub stub = PipelineMonitorServiceGrpc.newStub(channel);

        PipelineMonitorRequest request = PipelineMonitorRequest.newBuilder()
                .setKey(key)
                .setValue(value)
                .build();

        ListenableFuture<PipelineMonitorResponse> responseFuture = stub.monitorPipeline(request);

        try {
            PipelineMonitorResponse response = responseFuture.get();
            System.out.println("Received response from GRPC server: " + response.getMessage());
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Error sending to GRPC server: " + e.getMessage());
        }
    }

    private KafkaStreamsConfiguration getKafkaConfig() {
        Properties config = new Properties();
        config.setProperty(AbstractConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        config.setProperty(AbstractConfig.APPLICATION_ID_CONFIG, "pipeline_monitor_app");

        return new KafkaStreamsConfiguration(config);
    }

    public static void main(String[] args) {
        QDRM_Construct_AI_PipelineMonitor monitor = new QDRM_Construct_AI_PipelineMonitor();
        monitor.init();
    }
}