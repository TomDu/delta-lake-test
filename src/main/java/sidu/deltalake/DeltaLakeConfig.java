package sidu.deltalake;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DeltaLakeConfig {
    private Properties properties;
    private final String deltaTablePath;
    private final Configuration hadoopConf;

    public DeltaLakeConfig() {
        initProperties();
        System.setProperty("hadoop.home.dir", properties.getProperty("hadoop.home.dir"));

        String storageAccountName = properties.getProperty("storageAccountName");
        String storageAccountKey = properties.getProperty("storageAccountKey");
        String storageContainerName = properties.getProperty("storageContainerName");
        String deltaTableFolderName = properties.getProperty("deltaTableFolderName");

        deltaTablePath = String.format("wasbs://%s@%s.blob.core.windows.net/%s", storageContainerName, storageAccountName, deltaTableFolderName);

        hadoopConf = new Configuration();
        hadoopConf.set(String.format("fs.azure.account.key.%s.blob.core.windows.net", storageAccountName), storageAccountKey);
        hadoopConf.set("io.delta.standalone.LOG_STORE_CLASS_KEY", "io.delta.standalone.internal.storage.AzureLogStore");
    }

    public String getDeltaTablePath() {
        return deltaTablePath;
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    private void initProperties() {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
