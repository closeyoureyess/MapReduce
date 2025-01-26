import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static constants.ConstantsClass.*;


public class Worker {

    /* private final List<KeyValue> synchronizedKeyValueList;*/
    /*private final List<String> synchronizedStringList;*/
    /*private final Map<String, List<String>> synchronizedStringMap;*/
    private final ExecutorService executorService;
    private final ReentrantLock reentrantLock;
    private AtomicInteger atomicCounter;
    private final int numberOfTasks;

    public Worker(ExecutorService executorService, int numberOfTasks) {
        this.executorService = executorService;
        this.reentrantLock = new ReentrantLock();
        /*this.synchronizedKeyValueList = Collections.synchronizedList(new ArrayList<>());*/
        /*this.synchronizedStringList = Collections.synchronizedList(new ArrayList<>());*/
        /*this.synchronizedStringMap = Collections.synchronizedMap(new HashMap<>());*/
        this.atomicCounter = new AtomicInteger(ZERO);
        this.numberOfTasks = numberOfTasks;
    }

    public List<KeyValue> map(String fileName, String content) throws IOException {
        // разделяем content на слова и для каждого слова создаем объект KeyVal
        // где key - это слово, а value - это "1". И возвращаем список этих объектов
        reentrantLock.lock();
        List<KeyValue> synchronizedKeyValueList = Collections.synchronizedList(new ArrayList<>());
        try {
            fillSplitWordsToSynchronizedList(content, synchronizedKeyValueList);
            writeWordsToFiles(synchronizedKeyValueList);
        } finally {
            reentrantLock.unlock();
        }
        return synchronizedKeyValueList;
    }

    private String reduce(String key, List<String> values) {
        return String.valueOf(values);
    }

    public Path collectFragmentedFiles(String key, List<String> values) throws IOException {
        reentrantLock.lock();
        try {
            String rowForWrite = key + SPACE + reduce(key, values);
            return Files.write(Path.of("resultFile.txt"), rowForWrite.getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } finally {
            reentrantLock.unlock();
        }
    }

    public Map<String, List<String>> grabValuesForSameKeyFromFragmentedFiles(String key, List<Path> listAllMatchingFiles) throws IOException {
        reentrantLock.lock();
        Map<String, List<String>> synchronizedStringMap = Collections.synchronizedMap(new HashMap<>());
        List<String> synchronizedStringList = Collections.synchronizedList(new ArrayList<>());
        Actions actions = new Actions();
        try {
            for (Path path : listAllMatchingFiles) { // Перебрать все Path, которые ведут к промежуточным файлам
                String fileName = path.getFileName().toString(); // Получить имя файла
                fileName = actions.trimFileNameAndGetReduceKey(fileName);
                if (fileName.contains(key)) { // Если имя файла содержит ключ(номер reduce задачи, Y)
                    String line = Files.readString(path); // Вытащить ключ, значение из файла
                    String[] words = line.split("\\s+");
                    for (String word : words) {
                        Pattern pattern = Pattern.compile(".*\\d+.*", Pattern.DOTALL);
                        Matcher matcher = pattern.matcher(word);
                        boolean containsNumber = matcher.matches(); // Проеврить, яв-тся ли полученный элемент цифрой(1)
                        if (containsNumber) {
                            synchronizedStringList.add(word); // Добавить в List
                        }
                    }
                }
            }
            synchronizedStringMap.put(key, synchronizedStringList); // Добавить ключ, List со значениями в Map
            return synchronizedStringMap;
        } finally {
            reentrantLock.unlock();
        }
    }

    private void writeWordsToFiles(List<KeyValue> synchronizedKeyValueList) throws IOException {
        String finalRowForWrite;
        String fileName;
        String y;
        for (KeyValue keyValue : synchronizedKeyValueList) {
            String key = keyValue.getKey();
            int hashCode = key.hashCode();
            y = String.valueOf(hashCode % numberOfTasks);
            finalRowForWrite = key + SPACE + keyValue.getValue() + System.lineSeparator();
            fileName = PREFIX_FILE_NAME + DASH + atomicCounter.get() + DASH + y;

            Files.write(Path.of(fileName), finalRowForWrite.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private void fillSplitWordsToSynchronizedList(String content, List<KeyValue> synchronizedKeyValueList) {
        atomicCounter.getAndIncrement();
        String[] words = content.split(SPACE);
        for (String word : words) {
            KeyValue keyValue = new KeyValue(word, ONE_NUMBER_STRING);
            synchronizedKeyValueList.add(keyValue);
        }
    }
}
