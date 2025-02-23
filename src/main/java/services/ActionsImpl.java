package services;

import static constants.ConstantsClass.DASH;

public class ActionsImpl implements Actions {

    /**
     * Метод, обрезающий имя файла, позволяющий получить reduce-ключ из названия файда
     *
     * @param fileName Имя файла
     * @return reduce-ключ из названия файла
     */
    @Override
    public String trimFileNameAndGetReduceKey(String fileName) {
        int lastIndexPlusOne = fileName.lastIndexOf(DASH) + 1;
        int maxLenght = fileName.length();
        return fileName.substring(lastIndexPlusOne, maxLenght);
    }
}
