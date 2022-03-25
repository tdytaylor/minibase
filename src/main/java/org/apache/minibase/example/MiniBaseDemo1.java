package org.apache.minibase.example;


import org.apache.minibase.Bytes;
import org.apache.minibase.Config;
import org.apache.minibase.KeyValue;
import org.apache.minibase.MStore;
import org.apache.minibase.MiniBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * @Use
 * @Author: jeff
 * @Date: 2022/3/22 9:03
 */
public class MiniBaseDemo1 {

    private static final Logger LOG = LoggerFactory.getLogger(MiniBaseDemo1.class);

    public static void main(String[] args) throws IOException {

        //String dataDir = "data/minihbase-" + System.currentTimeMillis();
        String dataDir = "data/minihbase-1648001080385";
        File f = new File(dataDir);
        assert f.exists() || f.mkdirs();

        Config conf = new Config().setDataDir(dataDir).setMaxMemstoreSize(1024).setFlushMaxRetries(1).setMaxDiskFiles(10);
        final MiniBase db = MStore.create(conf).open();

        Scanner scanner = new Scanner(System.in);
        // Put
        boolean shutdown = false;
        while (!shutdown) {
            String line = scanner.nextLine();
            String[] strings = line.split("\\s");
            switch (strings[0]) {
                case "put" -> {
                    LOG.info("put key=" + strings[1] + " value=" + strings[2]);
                    db.put(Bytes.toBytes(strings[1]), Bytes.toBytes(strings[2]));
                }
                case "get" -> {
                    KeyValue keyValue = db.get(Bytes.toBytes(strings[1]));
                    if (keyValue == null || keyValue.getValue() == null) {
                        LOG.info("key=" + strings[1] + " not found");
                    } else {
                        LOG.info("get key=" + strings[1] + " value=" + new String(keyValue.getValue(), StandardCharsets.UTF_8));
                    }
                }
                case "delete" -> {
                    LOG.info("delete key=" + strings[1]);
                    db.delete(Bytes.toBytes(strings[1]));
                }
                case "exit" -> shutdown = true;
                default -> System.out.println("无法识别的指令。");
            }
        }

        // Scan
        //MiniBase.Iter<KeyValue> kv = db.scan();
        //while (kv.hasNext()) {
        //    KeyValue next = kv.next();
        //    System.out.println("key=" + new String(next.getKey(), StandardCharsets.UTF_8) +
        //            " , value=" +
        //            new String(next.getValue(), StandardCharsets.UTF_8));
        //}


        db.close();
    }
}
