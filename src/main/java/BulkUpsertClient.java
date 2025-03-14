import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import com.yandex.ydb.table.v1.TableServiceGrpc;
import com.yandex.ydb.table.YdbTable;
//import com.yandex.ydb.table.v1.YdbTableV1;



public class BulkUpsertClient {
    private final ManagedChannel channel;
    private final TableServiceGrpc.TableServiceBlockingStub blockingStub;

    public BulkUpsertClient(String host, int port) {
        // Create a channel to the server
        channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext() // For testing, no encryption
            .build();
        
        // Create a blocking stub - a synchronous client
        blockingStub = TableServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Call the bulkUpsert endpoint and print the result
     */
    public void callBulkUpsert() {
        System.out.println("Calling bulkUpsert...");
        
        // Create a sample request
        YdbTable.BulkUpsertRequest request = YdbTable.BulkUpsertRequest.newBuilder()
            .setTable("sample_table")
            // You can populate the rows with your actual data if needed
            // .setRows(...)
            .build();
            
        try {
            // Make the RPC call and get the response
            YdbTable.BulkUpsertResponse response = blockingStub.bulkUpsert(request);
            
            // Print response information
            System.out.println("BulkUpsert call successful!");
            System.out.println("Response: " + response.toString());
            
        } catch (Exception e) {
            System.err.println("RPC failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) throws Exception {
        // Default to localhost:8980 if no arguments provided
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 50051; // 8980
        
        BulkUpsertClient client = new BulkUpsertClient(host, port);
        try {
            client.callBulkUpsert();
        } finally {
            client.shutdown();
        }
    }
}