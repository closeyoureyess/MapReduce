import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static constants.ConstantsClass.*;


public class WorkerImpl implements Worker {

    private final ReentrantLock reentrantLock;
    private AtomicInteger atomicCounter;
    private final int numberOfTasks;
    private final Set<String> synchronizedStringSet;

    public WorkerImpl(int numberOfTasks) {
        this.reentrantLock = new ReentrantLock();
        this.synchronizedStringSet = Collections.synchronizedSortedSet(new TreeSet<>());
        this.atomicCounter = new AtomicInteger(ZERO);
        this.numberOfTasks = numberOfTasks;
    }

    @Override
    public List<KeyValue> map(String fileName, String content) throws IOException {
        // разделяем content на слова и для каждого слова создаем объект KeyVal
        // где key - это слово, а value - это "1". И возвращаем список этих объектов
        reentrantLock.lock();
        List<KeyValue> synchronizedKeyValueList = Collections.synchronizedList(new ArrayList<>());
        try {
            fillSplitWordsToSynchronizedList(content, synchronizedKeyValueList); // Метод, дробящий строку на ключи, записывающий в List ключ и единицу в виде значения
            writeWordsToFiles(synchronizedKeyValueList); // Метод, записывающий информацию в промежуточные файлы
        } finally {
            reentrantLock.unlock();
        }
        return synchronizedKeyValueList;
    }

    private String reduce(String key, List<String> values) {
        return String.valueOf(values);
    }

    /**
     * Метод, формирующий из переданного ключа(в виде, полученном в map) и List со списком эл-ов, т.е кол-ва встречаемости ключа в файлах,
     * строку для записи в итоговый файл, помещающий эту строку в синхронизированную коллекцию. Вызывается также метод reduce, который превращает List с эл-ми
     * встречаемости ключа в строку, которая потом будет записана в итоговый файл
     *
     *
     * @param key ключ(в виде, полученном в map)
     * @param values List со списком эл-ов, сколько раз этот ключ встречается в файлах
     * @return true - эл-т успешно добавлен в синхронизрованную коллекцию, поредназначенную для агрегации результатов
     * @throws IOException
     */
    @Override
    public Boolean collectFragmentedFiles(String key, List<String> values) throws IOException {
        reentrantLock.lock();
        try {
            String rowForWrite = key + SPACE + reduce(key, values)/* + System.lineSeparator()*/;
            return synchronizedStringSet.add(rowForWrite);
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Метод, предназначенный для записи в однопоточном режиме агрегированных данных из синхронизированной коллеции Set<String> в итоговый файл
     *
     * @return Путь к файлу, куда записали информацию
     * @throws IOException
     */
    @Override
    public Path writeOverdoneElementsToFile() throws IOException {
        return Files.write(Path.of("resultFile.txt"), synchronizedStringSet, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }

    /**
     * Метод, проверяющий все файлы с тем же reduce-ключом на наличие такого же ключа, который передан в метод в виде String
     *
     * @param key Ключ в виде String(в том виде, в котором его в map взяли из файла)
     * @param listAllMatchingFiles List с путями к промежуточным файлам
     * @return {@link Map}  c ключом и кол-вом раз по единичке за каждый раз, когда это значение было найдено в файле
     * @throws IOException
     */
    @Override
    public Map<String, List<String>> grabValuesForSameKeyFromFragmentedFiles(String key, List<Path> listAllMatchingFiles) throws IOException {
        reentrantLock.lock();
        Map<String, List<String>> synchronizedStringMap = Collections.synchronizedMap(new TreeMap<>());
        List<String> synchronizedStringList = Collections.synchronizedList(new ArrayList<>());
        Actions actions = new ActionsImpl();
        try {
            for (Path path : listAllMatchingFiles) { // Перебрать все Path, которые ведут к промежуточным файлам
                String fileName = path.getFileName().toString(); // Получить имя файла
                fileName = actions.trimFileNameAndGetReduceKey(fileName); // Обрезать имя файла, чтобы получить reduce-ключ из названия
                String reduceKey = getBucket(key.hashCode()); // Получить reduce-ключ из переданного первоначального ключа, кот-ый нах-ся в виде String
                if (fileName.contains(reduceKey)) { // Если имя файла содержит ключ(номер reduce задачи, Y)
                    String line = Files.readString(path); // Вытащить ключи, значения из файла
                    String[] words = line.split("\\s+");
                    for (int i = 0; i < words.length; i++) {
                        if (i % 2 >= 1 && // Индекс элемента нечетный
                                (                      // ↓String является словом и этот String - переданый в метод grabValuesForSameKeyFromFragmentedFiles ключ↓
                                        regexFunc("^[a-zA-Zа-яА-ЯёЁ]+$", words[i - 1]) && words[i - 1].equals(key)
                                )
                                &&
                                regexFunc(".*\\d+.*", words[i])) { // Элемент явялется цифрой
                            synchronizedStringList.add(words[i]);
                        }
                    }
                }
            }
            synchronizedStringMap.put(key, synchronizedStringList);
            return synchronizedStringMap; // Map с ключом и кол-вом раз по единичке за каждый раз, когда это значение было найдено в файле
        } finally {
            reentrantLock.unlock();
        }
    }

    /**
     * Метод, вычисляющий Y у ключа, X с помощью атомарного счетчика, записывающий ключ-значение в промежуточный файл с соответствующим названием, вида
     * m-X-Y
     *
     * @param synchronizedKeyValueList синхронизированный List с парами key-value из переданного в координатор файла
     * @throws IOException
     */
    private void writeWordsToFiles(List<KeyValue> synchronizedKeyValueList) throws IOException {
        String finalRowForWrite;
        String fileName;
        String y;
        for (KeyValue keyValue : synchronizedKeyValueList) {
            String key = keyValue.getKey();
            int hashCode = key.hashCode();
            y = getBucket(hashCode); // Узнать reduce-ключ
            finalRowForWrite = key + SPACE + keyValue.getValue() + System.lineSeparator(); // Формирование строки key-value для записи в файл
            fileName = PREFIX_FILE_NAME + DASH + atomicCounter.get() + DASH + y; // Формирование названия для файла

            Files.write(Path.of(fileName), finalRowForWrite.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    /**
     * Метод, дробящий строку на ключи, записывающий в List ключ и единицу в виде значения
     *
     * @param content Строка, содержащая в себе весь текст из передаваемого в координатор файла
     * @param synchronizedKeyValueList синхронизированный List, куда будут записываться key-value
     */
    private void fillSplitWordsToSynchronizedList(String content, List<KeyValue> synchronizedKeyValueList) {
        atomicCounter.getAndIncrement();
        String[] words = content.split(SPACE); // Раздробить строку по пробелу
        for (String word : words) {
            KeyValue keyValue = new KeyValue(word, ONE_NUMBER_STRING); // Записать ключ и единицу в класс
            synchronizedKeyValueList.add(keyValue); // Объект поместить в List
        }
    }

    /**
     * Метод, высчитывающий reduce-ключ по переданному хэшкоду String-ключа из List<KeyValue>
     *
     * @param hashCode хэшкод ключа
     * @return высчитанный после деления хэшкода по модулю reduce-ключ
     */
    private String getBucket(int hashCode) {
        return String.valueOf(hashCode % numberOfTasks);
    }

    /**
     * Метод, проверяющий String на соответсвие регулярному выражению
     *
     * @param regex регулярное выражение
     * @param word слово
     * @return true, если String соответствует регулярному выражению
     */
    private boolean regexFunc(String regex, String word) {
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(word);
        return matcher.matches();
    }
}