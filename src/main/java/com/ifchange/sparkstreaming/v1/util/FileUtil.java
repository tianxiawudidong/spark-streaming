package com.ifchange.sparkstreaming.v1.util;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by Administrator on 2017/7/31.
 */
public class FileUtil {

    private static Logger logger = Logger.getLogger(FileUtil.class);

    /*
     * 把异常信息写入文件中
	 * @param str1 异常信息
	 * @param bw
	 * @param br
	 * @throws IOException
	 */
//    public static void writeFile(String str1, BufferedWriter bw, BufferedReader br) {
//        try {
//            String data = br.readLine();
//            if (null != data) {
//                bw.newLine();
//                bw.write(str1);
//                bw.flush();
//            } else {
//                bw.write(str1);
//                bw.flush();
//            }
//        } catch (IOException e) {
//            logger.info("....write file error...");
//        }
//    }

    /*
    * 把异常信息写入文件中
    * @param message 异常信息
    * @param filePath 文件名称
    * @throws IOException
    */
    public static void writeFile(String filePath, String message) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                boolean flag = file.createNewFile();
                logger.info("file create " + flag);
            }
            FileOutputStream writerStream = new FileOutputStream(file, true);
            FileInputStream fileInputStream = new FileInputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(writerStream, "UTF-8"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream, "UTF-8"));

            String data = br.readLine();
            if (null != data) {
                bw.newLine();
                bw.write(message);
                bw.flush();
            } else {
                bw.write(message);
                bw.flush();
            }
            bw.close();
            br.close();
        } catch (IOException e) {
            logger.info("....write file error...");
        }
    }
}
