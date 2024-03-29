package fileManager;

import java.io.*;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * class to manage the opening of the collection file
 */
public class FileOpener {

    /**
     * used to convert collection from .tsv to .zip
     */
    public static void compressFile() {
        try {
            File file = new File("docs/collection.tsv");
            FileInputStream fileInputStream = new FileInputStream(file);

            FileOutputStream fileOutputStream = new FileOutputStream("docs/collection.zip");
            ZipOutputStream zipOutputStream = new ZipOutputStream(fileOutputStream);

            ZipEntry entry = new ZipEntry(file.getName());
            entry.setMethod(ZipEntry.DEFLATED);
            zipOutputStream.putNextEntry(entry);

            byte[] buffer = new byte[1024];
            int len;
            while ((len = fileInputStream.read(buffer)) > 0) {
                zipOutputStream.write(buffer, 0, len);
            }

            zipOutputStream.closeEntry();
            zipOutputStream.close();
            fileInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * used to extract from zip the documents
     * @param path of the .zip collection
     * @return stream of data
     * @throws IOException
     */
    public static InputStream extractFromZip(String path) throws IOException {
        ZipFile zipFile = new ZipFile(path);
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        ZipEntry entry = entries.nextElement();
        InputStream stream = zipFile.getInputStream(entry);
        return stream;
    }
}
