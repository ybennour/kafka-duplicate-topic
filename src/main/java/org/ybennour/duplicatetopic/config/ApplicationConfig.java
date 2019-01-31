package org.ybennour.duplicatetopic.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationConfig {
    private static Logger logger = LoggerFactory.getLogger(ApplicationConfig.class);
    private static final Properties properties = new Properties();

    private ApplicationConfig() {
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static void load() {
        logger.info("Loading properties file");
        InputStream input = null;
        String filename = "application.properties";
        try {
            input = ApplicationConfig.class.getClassLoader().getResourceAsStream(filename);
            if (input == null) {
                logger.error("Unable to find file {} ", (Object)filename);
                throw new RuntimeException("Unable to find file " + filename);
            }
            properties.load(input);
        }
        catch (IOException e) {
            logger.error("Error while loading properties file. ", (Throwable)e);
            throw new RuntimeException("Error while loading properties file. ", e);
        }
        finally {
            if (input != null) {
                try {
                    input.close();
                }
                catch (IOException e) {
                    logger.error("Error while closing properties file. ", (Throwable)e);
                    throw new RuntimeException("Error while closing properties file. ", e);
                }
            }
        }
    }
}