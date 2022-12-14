package utility;

public class Utils {
    //method used to concatenate two byte arrays
    public static byte[] addByteArray(byte[] array1, byte[] array2){
        byte[] concatenatedArray = new byte[array1.length + array2.length];
        System.arraycopy(array1, 0, concatenatedArray, 0, array1.length);
        System.arraycopy(array2, 0, concatenatedArray, array1.length, array2.length);
        return concatenatedArray;
    }
}
