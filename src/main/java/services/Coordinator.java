package services;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface Coordinator {
    void coordinatorFunction(List<String> listWithFiles) throws IOException, InterruptedException, ExecutionException;
}
