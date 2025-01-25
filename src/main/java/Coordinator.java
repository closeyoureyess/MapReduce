import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static constants.ConstantsClass.*;

public class Coordinator {

    private final ExecutorService executorService;

    public Coordinator(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void coordinatorFunction(List<String> listWithFiles) throws IOException, InterruptedException, ExecutionException {
        List<Callable<List<KeyValue>>> listWithContent = new ArrayList<>();
        Worker worker = new Worker(executorService);
        pullTheTextFromFile(listWithFiles, listWithContent, worker); // Прочитать файлы, создать Callable с вызовом map для тредпула

        List<Future<List<KeyValue>>> futureMapFuncList = new ArrayList<>();
        if (!listWithContent.isEmpty()) {
            futureMapFuncList = executorService.invokeAll(listWithContent); // Вызвать map для дробления content на key-value
        }

        List<KeyValue> keyValueList = new ArrayList<>();
        handleMapFuncResult(keyValueList, futureMapFuncList); // Обработать Future-результат

        List<Path> listAllMatchingFiles = new ArrayList<>();
        handleIntermediateFiles(listAllMatchingFiles, listWithFiles.size()); // Получить раздробленные файлы из директории

        List<String> listWithSortedReduceKeyString = getReduceKeyFromFileName(listAllMatchingFiles); // Получить отсортированный список номеров reduce-задач(Y в названии файла

        List<Callable<Map<String, List<String>>>> listWithValuesFromFragmentedFiles = new ArrayList<>();
        prepareCallableListForGatherValueFromFragmentedFiles(listWithSortedReduceKeyString, listAllMatchingFiles, listWithValuesFromFragmentedFiles, worker);

        List<Future<Map<String, List<String>>>> futureGrabValuesFuncList = new ArrayList<>();
        if (!listWithValuesFromFragmentedFiles.isEmpty()) {
            futureGrabValuesFuncList = executorService.invokeAll(listWithValuesFromFragmentedFiles); // Сгруппировать ключ и все его значения из промежуточных файлов, вернуть Map с ключом
            // и списком значений для ключа в List
        }

        Map<String, List<String>> keyAndListValue = new HashMap<>();
        handleGrabValuesFunc(keyAndListValue, futureGrabValuesFuncList);

        List<Callable<Void>> listWithReduceFunc = new ArrayList<>();
        prepareCallableListForReduce(keyAndListValue, listWithReduceFunc, worker);

        if (!listWithReduceFunc.isEmpty()) {
            executorService.invokeAll(listWithReduceFunc); // Вызвать reduce для каждого ключа
        }
    }

    private void prepareCallableListForReduce(Map<String, List<String>> keyAndListValue, List<Callable<Void>> listWithReduceFunc,
                                              Worker worker) {
        for (Map.Entry<String, List<String>> mapEnty : keyAndListValue.entrySet()) {
            String value = mapEnty.getKey();
            List<String> key = mapEnty.getValue();
            listWithReduceFunc.add(() -> {
                worker.collectFragmentedFiles(value, key);
                return null;
            });
        }
    }

    private void handleGrabValuesFunc(Map<String, List<String>> keyAndListValue, List<Future<Map<String, List<String>>>> futureGrabValuesFuncList) throws ExecutionException, InterruptedException {
        while (!futureGrabValuesFuncList.isEmpty()) {
            for (int i = 0; i < futureGrabValuesFuncList.size(); i++) {
                Future<Map<String, List<String>>> future = futureGrabValuesFuncList.get(i);
                if (future.isDone()) {
                    Map<String, List<String>> result = future.get();
                    for (Map.Entry<String, List<String>> mapEnty : result.entrySet()) {
                        String value = mapEnty.getKey();
                        List<String> key = mapEnty.getValue();
                        keyAndListValue.put(value, key);
                    }
                    futureGrabValuesFuncList.remove(i); // Удаляем завершённое Future
                    i--; // Сдвигаем индекс, чтобы не пропустить следующий элемент
                }
            }
        }
    }

    private void prepareCallableListForGatherValueFromFragmentedFiles(List<String> listWithSortedReduceKeyString, List<Path> listWithListAllMatchingFiles,
                                                                      List<Callable<Map<String, List<String>>>> listWithValuesFromFragmentedFiles,
                                                                      Worker worker) {
        for (String key : listWithSortedReduceKeyString) {
            listWithValuesFromFragmentedFiles.add(() -> worker.grabValuesForSameKeyFromFragmentedFiles(key, listWithListAllMatchingFiles));
        }
    }

    private void pullTheTextFromFile(List<String> listWithFiles, List<Callable<List<KeyValue>>> listWithContent, Worker worker) throws IOException {
        for (String filePath : listWithFiles) {
            String content = Files.readString(Paths.get(filePath));
            listWithContent.add(() -> worker.map(filePath, content));
        }
    }

    private void handleMapFuncResult(List<KeyValue> keyValueList, List<Future<List<KeyValue>>> futureList) throws ExecutionException, InterruptedException {
        while (!futureList.isEmpty()) {
            for (int i = 0; i < futureList.size(); i++) {
                Future<List<KeyValue>> future = futureList.get(i);
                if (future.isDone()) {
                    List<KeyValue> result = future.get();
                    keyValueList.addAll(result); // Добавляем результат
                    futureList.remove(i); // Удаляем завершённое Future
                    i--; // Сдвигаем индекс, чтобы не пропустить следующий элемент
                }
            }
        }
    }

    private List<Path> handleIntermediateFiles(List<Path> listWithListAllMatchingFiles, int prefixSize) throws IOException {
        for (int i = 1; i <= prefixSize; i++) {
            String prefix = FULL_PREFIX_FILE + i;
            try (Stream<Path> files = Files.list(Path.of("C:\\Users\\vadim\\IdeaProjects\\MapReduce\\"))) {
                if (files != null) {
                    List<Path> matchingFiles = files.filter(Files::isRegularFile)
                            .filter(file -> file.getFileName().toString().startsWith(prefix))
                            .collect(Collectors.toList());
                    listWithListAllMatchingFiles.addAll(matchingFiles);
                }
            }
        }
        return listWithListAllMatchingFiles;
    }

    private List<String> getReduceKeyFromFileName(List<Path> listWithListAllMatchingFiles) throws IOException {
        List<Integer> listReduceKeyInteger = new ArrayList<>();
        String fileName;
        for (Path innerPath : listWithListAllMatchingFiles) {
            fileName = substringFileExtension(innerPath);
            Integer reduceKey = Integer.getInteger(fileName.substring(fileName.lastIndexOf(DASH) + 1, fileName.length() - 1)); // Получить число Y из наименования файа
            listReduceKeyInteger.add(reduceKey);
        }
        return listReduceKeyInteger.stream()
                .distinct().sorted().map(String::valueOf).toList();
    }

    private String substringFileExtension(Path path) {
        String fileName = path.getFileName().toString(); // Получить имя файла
        int dotIndex = fileName.lastIndexOf(DOT);
        return (dotIndex == -1) ? fileName : fileName.substring(ZERO, dotIndex); // Удалить расширение
    }
}