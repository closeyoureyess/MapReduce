import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static constants.ConstantsClass.FULL_PREFIX_FILE;

/**
 * 1.Вызывается метод pullTheTextFromFile(), читающий файлы, создающий Callable с вызовом метода map
 * 2.Результат обрабатывается в handleMapFuncResult(), key-value из разных потоков собраны в 1 List
 * 3.Вызывается метод handleIntermediateFiles(), выгружающий в List пути ко всем промежуточным файлам
 * 4.Вызывается removeDuplicatesKeys, получающий из List<KeyValue> отсортированный список ключей
 * 5.Вызывается prepareCallableListForGatherValueFromFragmentedFiles, создающий Callable с вызовом метода
 * grabValuesForSameKeyFromFragmentedFiles
 * 6.Результат обрабатывается в handleGrabValuesFunc, Map заполняется ключами и списками значений этого ключа из разных потоков
 * 7.Запускается deleteIntermediateFiles, удаляющий промежуточные файлы
 * 8.Вызывается prepareCallableListForReduce, создающий Callable с вызовом метода collectFragmentedFiles
 * 9.Вызывается writeOverdoneElementsToFile, записывающий результат из синхронизированной коллекции в WorkerImpl в файл в однопоточном режиме
 */
public class CoordinatorImpl implements Coordinator {

    private final ExecutorService executorService;

    public CoordinatorImpl(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void coordinatorFunction(List<String> listWithFiles) throws IOException, InterruptedException, ExecutionException {
        List<Callable<List<KeyValue>>> listWithContent = new ArrayList<>();
        Worker worker = new WorkerImpl(listWithFiles.size());
        pullTheTextFromFile(listWithFiles, listWithContent, worker); // Прочитать файлы, создать Callable с вызовом map для тредпула

        List<Future<List<KeyValue>>> futureMapFuncList = new ArrayList<>();
        if (!listWithContent.isEmpty()) {
            futureMapFuncList = executorService.invokeAll(listWithContent); // Вызвать map для дробления content на key-value
        }

        List<KeyValue> keyValueList = new ArrayList<>();
        handleMapFuncResult(keyValueList, futureMapFuncList); // Обработать Future-результат, собрать все key-value из разных потоков в 1 List

        List<Path> listAllMatchingFiles = new ArrayList<>();
        handleIntermediateFiles(listAllMatchingFiles, listWithFiles.size()); // Получить раздробленные файлы из директории

        List<String> listWithSortedKeys = removeDuplicatesKeys(keyValueList); // Получить отсортированный список ключей из List<KeyValue>

        List<Callable<Map<String, List<String>>>> listWithValuesFromFragmentedFiles = new ArrayList<>();
        prepareCallableListForGatherValueFromFragmentedFiles(listWithSortedKeys, listAllMatchingFiles, listWithValuesFromFragmentedFiles, worker);

        List<Future<Map<String, List<String>>>> futureGrabValuesFuncList = new ArrayList<>();
        if (!listWithValuesFromFragmentedFiles.isEmpty()) {
            futureGrabValuesFuncList = executorService.invokeAll(listWithValuesFromFragmentedFiles); // Сгруппировать ключ и все его значения из промежуточных файлов, вернуть Map с ключом
            // и списком значений для ключа в List
        }

        Map<String, List<String>> keyAndListValue = new TreeMap<>();
        handleGrabValuesFunc(keyAndListValue, futureGrabValuesFuncList); // Обработать результаты, заполнить Map ключами и списками значений и разных потоков
        deleteIntermediateFiles(listAllMatchingFiles); // Удалить временные файлы

        List<Callable<Boolean>> listWithReduceFunc = new ArrayList<>();
        prepareCallableListForReduce(keyAndListValue, listWithReduceFunc, worker);

        if (!listWithReduceFunc.isEmpty()) {
            executorService.invokeAll(listWithReduceFunc); // Вызвать reduce для каждого ключа
            worker.writeOverdoneElementsToFile(); // Записать в однопоточном режиме информацию в результирующий файл
        }
    }

    /**
     * Метод, удаляющий промежуточные файлы
     *
     * @param listAllMatchingFiles List с путями к временным файлам
     * @throws IOException
     */
    private void deleteIntermediateFiles(List<Path> listAllMatchingFiles) throws IOException {
        for (Path path : listAllMatchingFiles) {
            Files.deleteIfExists(path);
        }
    }

    /**
     * Метод, заполняющий List с Callable для запуска метода collectFragmentedFiles
     *
     * @param keyAndListValue Map с ключом и списком его значений-встречаемости в файлах
     * @param listWithReduceFunc - List с Callable для запуска метода collectFragmentedFiles
     */
    private void prepareCallableListForReduce(Map<String, List<String>> keyAndListValue, List<Callable<Boolean>> listWithReduceFunc,
                                              Worker worker) {
        for (Map.Entry<String, List<String>> mapEnty : keyAndListValue.entrySet()) {
            String key = mapEnty.getKey();
            List<String> values = mapEnty.getValue();
            listWithReduceFunc.add(() ->
                    worker.collectFragmentedFiles(key, values)
            );
        }
    }

    /**
     * Метод, получающий результаты многопоточного вызова метода collectFragmentedFiles, помещающий результаты в Map с ключом и списком его значений-встречаемости
     * в файлах
     *
     * @param keyAndListValue Map с ключом и списком его значений-встречаемости в файлах
     * @param futureGrabValuesFuncList List с Future, рез-ом многопоточного вызова метода collectFragmentedFiles, который нужно разобрать
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void handleGrabValuesFunc(Map<String, List<String>> keyAndListValue, List<Future<Map<String, List<String>>>> futureGrabValuesFuncList) throws ExecutionException, InterruptedException {
        while (!futureGrabValuesFuncList.isEmpty()) {
            for (int i = 0; i < futureGrabValuesFuncList.size(); i++) {
                Future<Map<String, List<String>>> future = futureGrabValuesFuncList.get(i);
                if (future.isDone()) {
                    Map<String, List<String>> result = future.get();
                    for (Map.Entry<String, List<String>> mapEnty : result.entrySet()) {
                        String key = mapEnty.getKey();
                        List<String> value = mapEnty.getValue();
                        keyAndListValue.put(key, value);
                    }
                    futureGrabValuesFuncList.remove(i); // Удаляем завершённое Future
                    i--; // Сдвигаем индекс, чтобы не пропустить следующий элемент
                }
            }
        }
    }

    /**
     * Метод, заполняющий List с Callable для запуска метода grabValuesForSameKeyFromFragmentedFiles
     *
     * @param listWithSortedKeys {@link List<String>} c ключами(в том виде, в котором их получили в map), отсортированными ы алфавитном порядке виде
     * @param listWithListAllMatchingFiles List с путями к временным файлам
     * @param listWithValuesFromFragmentedFiles List с Callable для запуска метода grabValuesForSameKeyFromFragmentedFiles
     */
    private void prepareCallableListForGatherValueFromFragmentedFiles(List<String> listWithSortedKeys, List<Path> listWithListAllMatchingFiles,
                                                                      List<Callable<Map<String, List<String>>>> listWithValuesFromFragmentedFiles,
                                                                      Worker worker) {
        for (String key : listWithSortedKeys) {
            listWithValuesFromFragmentedFiles.add(() -> worker.grabValuesForSameKeyFromFragmentedFiles(key, listWithListAllMatchingFiles));
        }
    }

    /**
     * Метод, читающий содержимое передаваемых файлов в одну строку, помещающий вызов метода map, в котором строка будет обрабатываться, в Callable, для передачи
     * информации в invokeAll
     *
     * @param listWithFiles   List с наименованием файлов, которые переданы для обработки
     * @param listWithContent List с Callable c вызовом метода map внутри
     * @param worker          Объект класса WorkerImpl, в котором нах-ся метод Map
     * @throws IOException
     */
    private void pullTheTextFromFile(List<String> listWithFiles, List<Callable<List<KeyValue>>> listWithContent, Worker worker) throws IOException {
        for (String filePath : listWithFiles) {
            String content = Files.readString(Paths.get(filePath));
            listWithContent.add(() -> worker.map(filePath, content));
        }
    }

    /**
     * Метод, записывающий в результирующий List информацию из List<Future>, в котором хранятся рез-ты выполнения функции map,
     * которую выполняли разные потоки
     *
     * @param keyValueList List с key-value, которые были получены разными потоками
     * @param futureList   List c Future-результатами выполнения метода map
     * @throws ExecutionException
     * @throws InterruptedException
     */
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

    /**
     * Метод, позволяющий обработать раздробленные ранее в методе map промежуточные файлы. В методе ищутся все файлы(вычисляются по префиксу в названии), записываются
     * в List
     *
     * @param listWithListAllMatchingFiles List, в который будут записаны найденные файлы
     * @param prefixSize                   кол-во файлов, которые были поданы на вход - т.е цифра-префикс X(m-X) в названии файла
     * @throws IOException
     */
    private void handleIntermediateFiles(List<Path> listWithListAllMatchingFiles, int prefixSize) throws IOException {
        for (int i = 1; i <= prefixSize; i++) {
            String prefix = FULL_PREFIX_FILE + i; // Префикс m- + цифра, которая является X названии файла, т.е номером map-задачи
            try (Stream<Path> files = Files.list(Path.of("C:\\Users\\vadim\\IdeaProjects\\MapReduce\\"))) {
                if (files != null) {
                    List<Path> matchingFiles = files.filter(Files::isRegularFile)
                            .filter(file -> file.getFileName().toString().startsWith(prefix)) // Найти и сделать List с файлами с таким префиксом
                            .toList();
                    listWithListAllMatchingFiles.addAll(matchingFiles);
                }
            }
        }
    }

    /**
     * Метод, получающий ключи из объектов KeyValue, сортирующий ключи в алфавитном порядке, возвращающий List только с этими ключами
     *
     * @param keyValueList List с парами key-value, которые ранее были получены в методе map
     * @return {@link List<String>} c ключами(в том виде, в котором их получили в map), отсортированными ы алфавитном порядке виде
     */
    private List<String> removeDuplicatesKeys(List<KeyValue> keyValueList) {
        return keyValueList.stream().map(KeyValue::getKey).distinct().sorted().toList();
    }
}