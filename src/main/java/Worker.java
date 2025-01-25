import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static constants.ConstantsClass.*;


public class Worker {

   /* private final List<KeyValue> synchronizedKeyValueList;*/
    /*private final List<String> synchronizedStringList;*/
    /*private final Map<String, List<String>> synchronizedStringMap;*/
    private final ExecutorService executorService;
    private final ReentrantLock reentrantLock;
    private AtomicInteger atomicInteger;

    public Worker(ExecutorService executorService) {
        this.executorService = executorService;
        this.reentrantLock = new ReentrantLock();
        /*this.synchronizedKeyValueList = Collections.synchronizedList(new ArrayList<>());*/
        /*this.synchronizedStringList = Collections.synchronizedList(new ArrayList<>());*/
        /*this.synchronizedStringMap = Collections.synchronizedMap(new HashMap<>());*/
        this.atomicInteger = new AtomicInteger(ZERO);
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

    public void collectFragmentedFiles(String key, List<String> values) throws IOException {
        reentrantLock.lock();
        try {
            String rowForWrite = key + SPACE + reduce(key, values);
            Files.write(Path.of("resultFile.txt"), rowForWrite.getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } finally {
            reentrantLock.unlock();
        }
    }

    public Map<String, List<String>> grabValuesForSameKeyFromFragmentedFiles(String key, List<Path> listAllMatchingFiles) throws IOException {
        reentrantLock.lock();
        Map<String, List<String>> synchronizedStringMap = Collections.synchronizedMap(new HashMap<>());
        List<String> synchronizedStringList = Collections.synchronizedList(new ArrayList<>());
        try {
            for (Path path : listAllMatchingFiles) { // Перебрать все Path, которые ведут к промежуточным файлам
                String fileName = path.getFileName().toString(); // Получить имя файла
                if (fileName.contains(key)) { // Если имя файла содержит ключ(номер reduce задачи, Y)
                    String line = Files.readString(path); // Вытащить ключ, значение из файла
                    String[] words = line.split(SPACE);
                    for (String word : words) {
                        boolean containsNumber = Pattern.matches(".*\\\\d+.*", word); // Проеврить, яв-тся ли полученный элемент цифрой(1)
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
            finalRowForWrite = key + SPACE + keyValue.getValue();
            y = String.valueOf(Math.abs(key.hashCode())/* % atomicInteger.get()*/);
            fileName = PREFIX_FILE_NAME + DASH + atomicInteger.get() + DASH + y;
            Files.write(Path.of(fileName), finalRowForWrite.getBytes());
        }
    }

    private void fillSplitWordsToSynchronizedList(String content, List<KeyValue> synchronizedKeyValueList) {
        atomicInteger.getAndIncrement();
        String[] words = content.split(SPACE);
        for (String word : words) {
            KeyValue keyValue = new KeyValue(word, ONE_NUMBER_STRING);
            synchronizedKeyValueList.add(keyValue);
        }
    }
}
