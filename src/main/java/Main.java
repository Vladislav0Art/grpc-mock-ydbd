import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import com.yandex.ydb.table.v1.TableServiceGrpc;
import com.yandex.ydb.table.v1.YdbTableV1;



class MockTableService extends TableServiceGrpc.TableServiceImplBase {
    // com.yandex.ydb.table.YdbTable.BulkUpsertRequest request
    @Override
    public void bulkUpsert(
            com.yandex.ydb.table.YdbTable.BulkUpsertRequest request,
            io.grpc.stub.StreamObserver<com.yandex.ydb.table.YdbTable.BulkUpsertResponse> responseObserver
    ) {
        System.out.println("Received bulkUpsert request: " + request.toString());
        // Create a mock response object for BulkUpsertResponse
        com.yandex.ydb.table.YdbTable.BulkUpsertResponse response =
                com.yandex.ydb.table.YdbTable.BulkUpsertResponse.newBuilder()
                        .build(); // Build an empty response

        // Send the response back to the client
        responseObserver.onNext(response);

        // Signal that the response is complete
        responseObserver.onCompleted();
    }

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