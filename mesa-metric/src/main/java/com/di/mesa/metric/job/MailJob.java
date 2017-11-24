package com.di.mesa.metric.job;

import com.di.mesa.metric.model.AlarmConcerner;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class MailJob implements MsgJob {

    private static String mail_host = "smtp.exmail.qq.com"; // mail服务器地址
    private static String mail_username = "monitor@company.com"; // 发送mail信箱的用户名
    private static String mail_password = "KdQWE@123";
    private static String charset = "gbk";
    private static final Logger logger = LoggerFactory.getLogger(MailJob.class);
    private static String self_mail = "yourname@company.com";

    /**
     * @param args
     */
    public static void main(String[] args) {

        String content = "Time:2015-08-21 16:28:00 Dimension:<b>SOLRREQUEST_ADS</b> Count:182 Value:<font color=#FF0000>106</font> [AVG_GREATER] Threshold:<b>100</b> In 1 Minutes.";
    }

    @Override
    public void sendMsg(AlarmConcerner alarmConcerners, String content) throws IOException {
        send("test", "监控告警-mesa实时告警", content, alarmConcerners.getMailTeller());
    }

    public void send(String username, String title, String content, List<String> mailList) {
        if (content == null || content.isEmpty() || mailList == null) {
            return;
        }
        content += " \n From: bigdata@company.com";
        HtmlEmail email = new HtmlEmail();
        //String[] mailList = mails.split(",");
        try {
            email.setHostName(mail_host);
            email.setSmtpPort(25);
            email.setCharset(charset);
            email.setAuthentication(mail_username, mail_password);
            email.setFrom(mail_username, username);
            for (String mail : mailList) {
                email.addTo(mail);
            }
            email.setSubject(title);
            email.setTextMsg(content);
            email.send();

        } catch (EmailException e) {
            logger.error(e.getMessage());
            try {
                email = new HtmlEmail();
                email.setHostName(mail_host);
                email.setSmtpPort(25);
                email.setCharset(charset);
                email.setAuthentication(mail_username, mail_password);
                email.setFrom(mail_username, username);
                email.addTo(self_mail);
                email.setSubject("Send monitor mail error!");
                email.setTextMsg("Error:" + e.getMessage() + "\n" + title + "\n" + content);
                email.send();
            } catch (EmailException e1) {
            }
            logger.error(" [send email error] title:" + title + " content:" + content + " mails:" + mailList);
        }
    }

    public void send(String username, String title, String content, String mails, File file) {
        if (content == null || content.isEmpty() || mails == null) {
            return;
        }
        content += " \n From: bigdata@company.com";
        HtmlEmail email = new HtmlEmail();
        String[] mailList = mails.split(",");
        try {
            email.setHostName(mail_host);
            email.setSmtpPort(25);
            email.setCharset(charset);
            email.setAuthentication(mail_username, mail_password);
            email.setFrom(mail_username, username);
            for (String mail : mailList) {
                email.addTo(mail);
            }
            email.setSubject(title);
            email.setTextMsg(content);

            MimeMultipart mimeMultipart = new MimeMultipart();
            BodyPart attachmentBodyPart = new MimeBodyPart();
            DataSource source = new FileDataSource(file);
            attachmentBodyPart.setDataHandler(new DataHandler(source));

            // 网上流传的解决文件名乱码的方法，其实用MimeUtility.encodeWord就可以很方便的搞定
            // 这里很重要，通过下面的Base64编码的转换可以保证你的中文附件标题名在发送时不会变成乱码
            // sun.misc.BASE64Encoder enc = new sun.misc.BASE64Encoder();
            // messageBodyPart.setFileName("=?GBK?B?" +
            // enc.encode(attachment.getName().getBytes()) + "?=");

            // MimeUtility.encodeWord可以避免文件名乱码
            attachmentBodyPart.setFileName(MimeUtility.encodeWord(file.getName()));
            mimeMultipart.addBodyPart(attachmentBodyPart);
            email.addPart(mimeMultipart);

            email.send();

        } catch (EmailException e) {
            logger.error(e.getMessage());
            try {
                email = new HtmlEmail();
                email.setHostName(mail_host);
                email.setSmtpPort(25);
                email.setCharset(charset);
                email.setAuthentication(mail_username, mail_password);
                email.setFrom(mail_username, username);
                email.addTo(self_mail);
                email.setSubject("Send monitor mail error!");
                email.setTextMsg("Error:" + e.getMessage() + "\n" + title + "\n" + content);
                email.send();
            } catch (EmailException e1) {
            }
            logger.error(" [send email error] title:" + title + " content:" + content + " mails:" + mails);
        } catch (MessagingException e) {
            e.printStackTrace();
            logger.error(" [send email error] title:" + title + " content:" + content + " mails:" + mails);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }


}
