import static constants.ConstantsClass.DASH;

public class Actions {

    public String trimFileNameAndGetReduceKey(String fileName) {
        int lastIndexPlusOne = fileName.lastIndexOf(DASH) + 1;
        int maxLenght = fileName.length();
        return fileName.substring(lastIndexPlusOne, maxLenght);
    }
}
