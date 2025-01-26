import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface Worker {
    List<KeyValue> map(String fileName, String content) throws IOException;
    Boolean collectFragmentedFiles(String key, List<String> values) throws IOException;
    Path writeOverdoneElementsToFile() throws IOException;
    Map<String, List<String>> grabValuesForSameKeyFromFragmentedFiles(String key, List<Path> listAllMatchingFiles) throws IOException;
}
