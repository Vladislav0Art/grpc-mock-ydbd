import com.yandex.ydb.StatusCodesProtos;
import com.yandex.ydb.discovery.DiscoveryProtos;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import com.yandex.ydb.table.v1.TableServiceGrpc;
import com.yandex.ydb.discovery.v1.DiscoveryServiceGrpc;
import com.yandex.ydb.discovery.DiscoveryProtos.ListEndpointsRequest;
import com.yandex.ydb.discovery.DiscoveryProtos.ListEndpointsResponse;
import com.yandex.ydb.table.v1.YdbTableV1;
import com.yandex.ydb.OperationProtos.Operation;
import com.google.protobuf.Any;
import com.yandex.ydb.table.YdbTable;

import java.util.Objects;


class MockDiscoveryService extends DiscoveryServiceGrpc.DiscoveryServiceImplBase {
    // com.yandex.ydb.discovery.v1.ListEndpointsRequest request
    private final String address;
    private final int port;

    MockDiscoveryService(String address, int port) {
        Objects.requireNonNull(address);
        this.address = address;
        this.port = port;
    }

    @Override
    public void listEndpoints(ListEndpointsRequest request, StreamObserver<ListEndpointsResponse> responseObserver
    ) {
        System.out.println("Received listEndpoints request: " + request.toString());

        DiscoveryProtos.EndpointInfo endpoint = DiscoveryProtos.EndpointInfo.newBuilder()
                .setAddress(address)
                .setPort(port)
                .build();

        DiscoveryProtos.ListEndpointsResult result = DiscoveryProtos.ListEndpointsResult.newBuilder()
                .addEndpoints(endpoint)
                .build();

        Operation operation = Operation.newBuilder()
                .setReady(true)
                .setStatus(StatusCodesProtos.StatusIds.StatusCode.SUCCESS)
                .setResult(Any.pack(result))
                .build();

        ListEndpointsResponse response = ListEndpointsResponse.newBuilder()
                .setOperation(operation)
                // .addEndpoints(YdbTableV1.YdbTableService.newBuilder().setPath("/rpc/v1/table/sessionService").build())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}


class MockTableService extends TableServiceGrpc.TableServiceImplBase {
    // com.yandex.ydb.table.YdbTable.BulkUpsertRequest request

    @Override
    public void createSession(
        YdbTable.CreateSessionRequest request,
        io.grpc.stub.StreamObserver<YdbTable.CreateSessionResponse> responseObserver) {
        System.out.println("Received createSession request: " + request.toString());

        YdbTable.CreateSessionResult result = YdbTable.CreateSessionResult.newBuilder()
                .setSessionId("test_session_id")
                .build();

        Operation operation = Operation.newBuilder()
                .setReady(true)
                .setStatus(StatusCodesProtos.StatusIds.StatusCode.SUCCESS)
                .setResult(Any.pack(result))
                .build();

        YdbTable.CreateSessionResponse response = YdbTable.CreateSessionResponse.newBuilder()
                .setOperation(operation)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void bulkUpsert(
        YdbTable.BulkUpsertRequest request,
        io.grpc.stub.StreamObserver<YdbTable.BulkUpsertResponse> responseObserver
    ) {
        System.out.println("Received bulkUpsert request: " + request.toString());

        YdbTable.BulkUpsertResponse response = YdbTable.BulkUpsertResponse.newBuilder().build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}


public class Main {
    public static void main(String[] args) throws Exception {
        // Create the gRPC server on a specific port
        int port = 50051;
        String address = "localhost";

        Server server = ServerBuilder.forPort(port)
                .addService(new MockTableService())
                .addService(new MockDiscoveryService(address, port))
                .build();

        // Start the server
        server.start();
        System.out.println("Server started, listening on " + port);

        // Keep the server running
        server.awaitTermination();
    }
}