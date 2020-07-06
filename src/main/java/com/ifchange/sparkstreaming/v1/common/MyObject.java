/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ifchange.sparkstreaming.v1.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 *
 * @author root
 */
public class MyObject extends Object {

    /**
     * 空值检查
     *
     * @param pInput 要检查的字符串
     * @return boolean 返回检查结果,但传入的字符串为空的场合,返回真
     */
    public static boolean isNull(Object pInput) {
        // 判断参数是否为空或者’’
        if (pInput == null || "’’".equals(pInput)) {
            return true;
        } else if ("java.lang.String".equals(pInput.getClass().getName())) {
            // 判断传入的参数的String类型的
            // 替换各种空格
            String tmpInput = Pattern.compile("[““r|““n|““u3000]")
                    .matcher((String) pInput).replaceAll("");
            // 匹配空           
            return Pattern.compile("^(““s)*$").matcher(tmpInput).matches();
        } else {
            // 方法类
            Method method = null;
            try {
                // 访问传入参数的size方法
                method = pInput.getClass().getMethod("size");
                // 判断size大小
                // size为0的场合
                if (Integer.parseInt(String.valueOf(method.invoke(pInput))) == 0) {
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                // 访问失败
                try {
                    // 访问传入参数的getItemCount方法
                    method = pInput.getClass().getMethod("getItemCount");
                    // 判断size大小
                    // getItemCount为0的场合
                    if (Integer.parseInt(String.valueOf(method.invoke(pInput))) == 0) {
                        return true;
                    } else {
                        return false;
                    }
                } catch (Exception ex) {
                    // 访问失败
                    try {
                        // 判断传入参数的长度
                        // 长度为0的场合
                        if (Array.getLength(pInput) == 0) {
                            return true;
                        } else {
                            return false;
                        }
                    } catch (Exception exx) {
                        // 访问失败
                        try {
                            // 访问传入参数的hasNext方法
                            method = Iterator.class.getMethod("hasNext");
                            // 转换hasNext的值
                            return !Boolean.valueOf(String.valueOf(method.invoke(pInput))) ? true : false;
                        } catch (Exception exxx) {
                            // 以上场合不满足返回假
                            return false;
                        }
                    }
                }
            }
        }
    }

    /**
     * 空值检查
     *
     * @param pInput 要检查的字符串
     * @return boolean 返回检查结果,但传入的字符串为空的场合,返回真
     */
    public static boolean isEmpty(Object pInput) {
        // 判断参数是否为空或者’’
        if (pInput == null || "’’".equals(pInput)) {
            return true;
        } else if ("java.lang.String".equals(pInput.getClass().getName())) {

            // 判断传入的参数的String类型的
            // 替换各种空格
            String tmpInput = Pattern.compile("[““r|““n|““u3000|0|0.0]").matcher((String) pInput).replaceAll("");
            // 匹配空           
            return Pattern.compile("^(““s)*$").matcher(tmpInput).matches();

        } else {
            // 方法类
            Method method = null;
            try {
                // 访问传入参数的intValue方法

                method = pInput.getClass().getMethod("intValue");
                if (Integer.parseInt(String.valueOf(method.invoke(pInput))) == 0) {
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e1) {
                try {
                    // 访问传入参数的size方法
                    method = pInput.getClass().getMethod("size");
                    // 判断size大小
                    // size为0的场合
                    if (Integer.parseInt(String.valueOf(method.invoke(pInput))) == 0) {
                        return true;
                    } else {
                        return false;
                    }
                } catch (Exception e) {
                    // 访问失败
                    try {
                        // 访问传入参数的getItemCount方法
                        method = pInput.getClass().getMethod("getItemCount");
                        // 判断size大小
                        // getItemCount为0的场合
                        if (Integer.parseInt(String.valueOf(method.invoke(pInput))) == 0) {
                            return true;
                        } else {
                            return false;
                        }
                    } catch (Exception ex) {
                        // 访问失败
                        try {
                            // 判断传入参数的长度
                            // 长度为0的场合
                            if (Array.getLength(pInput) == 0) {
                                return true;
                            } else {
                                return false;
                            }
                        } catch (Exception exx) {
                            // 访问失败
                            try {
                                // 访问传入参数的hasNext方法
                                method = Iterator.class.getMethod("hasNext");
                                // 转换hasNext的值
                                return !Boolean.valueOf(String.valueOf(method.invoke(pInput))) ? true : false;
                            } catch (Exception exxx) {
                                // 以上场合不满足返回假
                                return false;
                            }
                        }
                    }
                }
            }
        }
    }
    //java 合并两个byte数组  

    public static byte[] byteMerger(byte[] byte_1, byte[] byte_2) {
        byte[] byte_3 = null;
        if (byte_1 != null && byte_2 != null && byte_1.length > 0 && byte_2.length > 0) {
            byte_3 = new byte[byte_1.length + byte_2.length];
            System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
            System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
        } else if (byte_1 != null && byte_1.length > 0) {
            byte_3 = new byte[byte_1.length];
            System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
        } else if (byte_2 != null && byte_2.length > 0) {
            byte_3 = new byte[byte_2.length];
            System.arraycopy(byte_2, 0, byte_3, 0, byte_2.length);
        }
        return byte_3;
    }

    public String execPHP(String scriptName, String param) {
        StringBuilder output = new StringBuilder();
        BufferedReader input = null;
        String phpPath = "/usr/local/php/bin/php";
        try {
            String line;
            Process p = Runtime.getRuntime().exec(phpPath + " " + scriptName + " " + param);
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) {
                output.append(line + "\n");
                //p.destroy();
            }

            if (line == null) {
                p.destroy();
            }
        } catch (Exception err) {
            err.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return output.toString();
    }
}
