import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) {
        try (ExecutorService executorService = Executors.newCachedThreadPool()) {
            Coordinator coordinator = new Coordinator(executorService);
            coordinator.coordinatorFunction(List.of("newTest.txt", "newTest2.txt", "newTest3.txt"));
        } catch (InterruptedException | IOException | ExecutionException e ) {
            System.out.println("Error" + e);
        }
    }
}
