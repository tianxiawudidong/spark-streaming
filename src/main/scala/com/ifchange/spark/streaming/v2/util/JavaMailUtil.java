package com.ifchange.spark.streaming.v2.util;

import javax.mail.Address;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

public class JavaMailUtil {




    public static void main(String[] args) throws Exception {
        InternetAddress[] receiveMails = new InternetAddress[1];
        receiveMails[0] = new InternetAddress("xudongjun123456@163.com", "", "UTF-8");
        String subject = "ICDC日志报警2";
        String content = "ICDC日志报警2";
        JavaMailUtil.sendEmailByIfchange(subject, content,receiveMails);
    }

    /*
     *
     * @param receiveMail 收件人
     * @param subject  邮件主题
     * @param content  邮件内容
     */
    public static void sendEmailByCheng95(Address[] receiveMail, String subject, String content) throws Exception {
        if (null == receiveMail) {
            throw new Exception("收件人为空!!!");
        }
        String myEmailAccount = "dongjun.xu@cheng95.com";
        String myEmailPassword = "ifchange888";
        String myEmailSMTPHost = "service.cheng95.com";
        Properties props = new Properties();                    // 参数配置
        props.setProperty("mail.transport.protocol", "smtp");   // 使用的协议（JavaMail规范要求）
        props.setProperty("mail.smtp.host", myEmailSMTPHost);   // 发件人的邮箱的 SMTP 服务器地址
        props.setProperty("mail.smtp.auth", "true");
        Session session = Session.getDefaultInstance(props);
//        session.setDebug(true);
        // 1. 创建一封邮件
        MimeMessage message = new MimeMessage(session);
        // 2. From: 发件人（昵称有广告嫌疑，避免被邮件服务器误认为是滥发广告以至返回失败，请修改昵称）
        message.setFrom(new InternetAddress(myEmailAccount, "日志监控服务器", "UTF-8"));
        // 3. To: 收件人（可以增加多个收件人、抄送、密送）
//        message.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(receiveMail, "", "UTF-8"));
        message.setRecipients(MimeMessage.RecipientType.TO, receiveMail);
        // 4. Subject: 邮件主题（标题有广告嫌疑，避免被邮件服务器误认为是滥发广告以至返回失败，请修改标题）
        message.setSubject(subject, "UTF-8");

        // 5. Content: 邮件正文（可以使用html标签）（内容有广告嫌疑，避免被邮件服务器误认为是滥发广告以至返回失败，请修改发送内容）
        message.setContent(content, "text/html;charset=UTF-8");

        // 6. 设置发件时间
        message.setSentDate(new Date());

        // 7. 保存设置
        message.saveChanges();
        // 4. 根据 Session 获取邮件传输对象
        Transport transport = session.getTransport();

        transport.connect(myEmailAccount, myEmailPassword);

        // 6. 发送邮件, 发到所有的收件地址, message.getAllRecipients() 获取到的是在创建邮件对象时添加的所有收件人, 抄送人, 密送人
        transport.sendMessage(message, message.getAllRecipients());

        // 7. 关闭连接
        transport.close();
    }

    /*
    *
    * @param receiveMail 收件人
    * @param subject  邮件主题
    * @param content  邮件内容
    */
    public static void sendEmailByIfchange(String subject, String content,InternetAddress[] receiveMail) throws Exception {
        if (null == receiveMail) {
            throw new Exception("收件人为空!!!");
        }
        String myEmailAccount2 = "dongjun.xu@ifchange.com";
        String myEmailPassword2 = "";
        String myEmailSMTPHost2 = "smtp.exmail.qq.com";
        Properties props = new Properties();                    // 参数配置
        props.setProperty("mail.transport.protocol", "smtp");   // 使用的协议（JavaMail规范要求）
        props.setProperty("mail.smtp.host", myEmailSMTPHost2);   // 发件人的邮箱的 SMTP 服务器地址
        props.setProperty("mail.smtp.auth", "true");
        Session session = Session.getDefaultInstance(props);
//        session.setDebug(true);
        // 1. 创建一封邮件
        MimeMessage message = new MimeMessage(session);
        // 2. From: 发件人（昵称有广告嫌疑，避免被邮件服务器误认为是滥发广告以至返回失败，请修改昵称）
        message.setFrom(new InternetAddress(myEmailAccount2, "日志监控服务器", "UTF-8"));
        // 3. To: 收件人（可以增加多个收件人、抄送、密送）
//        message.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(receiveMail, "", "UTF-8"));
        message.setRecipients(MimeMessage.RecipientType.TO, receiveMail);
        // 4. Subject: 邮件主题（标题有广告嫌疑，避免被邮件服务器误认为是滥发广告以至返回失败，请修改标题）
        message.setSubject(subject, "UTF-8");

        // 5. Content: 邮件正文（可以使用html标签）（内容有广告嫌疑，避免被邮件服务器误认为是滥发广告以至返回失败，请修改发送内容）
        message.setContent(content, "text/html;charset=UTF-8");

        // 6. 设置发件时间
        message.setSentDate(new Date());

        // 7. 保存设置
        message.saveChanges();
        // 4. 根据 Session 获取邮件传输对象
        Transport transport = session.getTransport();

        // 5. 使用 邮箱账号 和 密码 连接邮件服务器, 这里认证的邮箱必须与 message 中的发件人邮箱一致, 否则报错
        transport.connect(myEmailAccount2, myEmailPassword2);

        // 6. 发送邮件, 发到所有的收件地址, message.getAllRecipients() 获取到的是在创建邮件对象时添加的所有收件人, 抄送人, 密送人
        transport.sendMessage(message, message.getAllRecipients());

        // 7. 关闭连接
        transport.close();
    }


}
