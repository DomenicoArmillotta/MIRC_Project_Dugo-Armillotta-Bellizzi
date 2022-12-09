package fileManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class FileOpener {
    public void extractFromZip() throws IOException {
        ZipFile zipFile = new ZipFile("C:/test.zip");

        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        //we look for just an element
        while(entries.hasMoreElements()){
            ZipEntry entry = entries.nextElement();
            InputStream stream = zipFile.getInputStream(entry);
        }
        //TODO: continue
    }
}
