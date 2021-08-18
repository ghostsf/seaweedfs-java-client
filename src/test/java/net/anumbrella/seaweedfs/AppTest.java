package net.anumbrella.seaweedfs;

import net.anumbrella.seaweedfs.core.ConnectionProperties;
import net.anumbrella.seaweedfs.core.FileSource;
import net.anumbrella.seaweedfs.core.FileTemplate;
import net.anumbrella.seaweedfs.core.file.FileHandleStatus;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class AppTest {
    private static FileSource fileSource = new FileSource();
    @Before
    public void setUp() {
        ConnectionProperties connectionProperties
                = new ConnectionProperties.Builder().host("localhost").port(9333).connectionTimeout(3600).maxConnection(100).build();
        fileSource.setProperties(connectionProperties);
        fileSource.startup();
    }

    @Test
    public void run() {
        FileTemplate fileTemplate = new FileTemplate(fileSource.getConnection());
        FileHandleStatus fileHandleStatus = null;
        try {
            fileHandleStatus = fileTemplate.saveFileByStream("xxx.sql", new File("/Users/ghostsf/Downloads/xxx.sql"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(fileHandleStatus.getFileUrl());
    }
}
