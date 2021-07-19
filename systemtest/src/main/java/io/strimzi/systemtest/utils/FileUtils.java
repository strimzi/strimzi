/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class FileUtils {

    private static final Logger LOGGER = LogManager.getLogger(FileUtils.class);

    private FileUtils() { }

    @SuppressFBWarnings({"RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE"})
    public static File downloadAndUnzip(String url) throws IOException {
        File dir = Files.createTempDirectory(FileUtils.class.getName()).toFile();

        try (InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
            ZipInputStream zin = new ZipInputStream(bais)) {

            dir.deleteOnExit();

            ZipEntry entry = zin.getNextEntry();
            byte[] buffer = new byte[8 * 1024];
            int len;
            while (entry != null) {
                File file = new File(dir, entry.getName());

                if (!file.toPath().normalize().startsWith(dir.toPath())) {
                    throw new RuntimeException("Invalid zip entry - unpacks outside of the destination path");
                }

                if (entry.isDirectory()) {
                    if (file.exists()) {
                        if (!file.isDirectory()) {
                            throw new IOException("Malformed zip file");
                        }
                    } else {
                        if (!file.mkdirs()) {
                            throw new IOException("Could not create directory " + file);
                        }
                    }
                } else {
                    file.getParentFile().mkdirs(); // create parent; in case zip file is malformed
                    try (FileOutputStream fout = new FileOutputStream(file)) {
                        while ((len = zin.read(buffer)) != -1) {
                            fout.write(buffer, 0, len);
                        }
                    }
                }
                entry = zin.getNextEntry();
            }
        } catch (RuntimeException e) {
            LOGGER.error("RuntimeException {}", e.getMessage());
        }
        return dir;
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public static File downloadYaml(String url) throws IOException {
        File yamlFile = File.createTempFile("temp-file", ".yaml");

        try (InputStream bais = (InputStream) URI.create(url).toURL().openConnection().getContent();
             BufferedReader br = new BufferedReader(new InputStreamReader(bais, StandardCharsets.UTF_8));
             OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(yamlFile), StandardCharsets.UTF_8)) {

            StringBuilder sb = new StringBuilder();

            String read;
            while ((read = br.readLine()) != null) {
                sb.append(read);
                sb.append("\n");
            }
            String yaml = sb.toString();

            osw.write(yaml);
            return yamlFile;

        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static File updateNamespaceOfYamlFile(String pathToOrigin, String namespace) throws IOException {
        byte[] encoded;
        File yamlFile = File.createTempFile("temp-file", ".yaml");

        try (OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(yamlFile), StandardCharsets.UTF_8)) {
            encoded = Files.readAllBytes(Paths.get(pathToOrigin));

            String yaml = new String(encoded, StandardCharsets.UTF_8);
            yaml = yaml.replaceAll("myproject", namespace);

            osw.write(yaml);
            return yamlFile.toPath().toFile();
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
        return null;
    }
}
