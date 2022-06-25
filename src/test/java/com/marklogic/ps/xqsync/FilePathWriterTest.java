package com.marklogic.ps.xqsync;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;

import static com.marklogic.ps.xqsync.Configuration.INPUT_CONNECTION_STRING_KEY;
import static com.marklogic.ps.xqsync.Configuration.OUTPUT_PATH_KEY;
import static org.junit.Assert.*;

public class FilePathWriterTest {

  @Test
  public void write() {
    String root = "/tmp";
    String filename = "test.txt";
    Properties properties = new Properties();
    properties.setProperty(OUTPUT_PATH_KEY, root);
    properties.setProperty(INPUT_CONNECTION_STRING_KEY, "xcc://xqsync-test-user:xqsync-test-password@localhost:8000");
    Configuration configuration = new Configuration();
    configuration.properties = properties;
    configuration.setLogger(SimpleLogger.getSimpleLogger());
    byte[] content = "test".getBytes(StandardCharsets.UTF_8);
    XQSyncDocumentMetadata metadata = new XQSyncDocumentMetadata();
    metadata.addCollection("foo");
    try {
      configuration.configure();
      FilePathWriter writer = new FilePathWriter(configuration);
      File outputFile = new File(root, filename);
      outputFile.deleteOnExit();

      writer.write(filename, content, metadata);

      assertTrue(outputFile.exists());
      assertTrue(new File(XQSyncDocument.getMetadataPath(outputFile)).exists());
      assertTrue(Arrays.equals(content, Utilities.cat(outputFile)));
    } catch (Exception ex) {
      fail();
    }
  }

  @Test
  public void writeFile() {
    Configuration configuration = new Configuration();
    FilePathWriter writer = new FilePathWriter(configuration);
    byte[] content = "test".getBytes(StandardCharsets.UTF_8);
    try {
      Path outputFilePath = Files.createTempFile("FilePathWriterTest", ".txt");
      File outputFile = outputFilePath.toFile();
      outputFile.deleteOnExit();
      writer.write(content, outputFile);
      assertTrue(outputFile.exists());
      assertTrue(Arrays.equals(content, Utilities.cat(outputFile)));
    } catch (IOException | SyncException ex) {
      fail();
    }
  }

  @Test (expected = FatalException.class)
  public void writeFileNoParent() {
    Configuration configuration = new Configuration();
    FilePathWriter writer = new FilePathWriter(configuration);
    byte[] content = "test".getBytes(StandardCharsets.UTF_8);
    try {
      File outputFile = new File("noParent.txt");
      outputFile.deleteOnExit();
      writer.write(content, outputFile);
    } catch (IOException | SyncException ex) {
      fail();
    }
  }
}