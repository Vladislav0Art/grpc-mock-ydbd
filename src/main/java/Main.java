import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import com.yandex.ydb.table.v1.TableServiceGrpc;



class MockTableService extends TableServiceGrpc.TableServiceImplBase {
}


public class Main {
    public static void main(String[] args) throws Exception {
        // Create the gRPC server on a specific port
        int port = 50051;
        Server server = ServerBuilder.forPort(port)
                .addService(new MockTableService()) // Add your service implementation here
                .build();

        // Start the server
        server.start();
        System.out.println("Server started, listening on " + port);

        // Keep the server running
        server.awaitTermination();
    }
}