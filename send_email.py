#!usr/bin/python
#-*-coding:UTF-8 -*-
import datetime
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr


class Email:
    def __init__(self, my_sender, my_user, password, SMTP, port=465):
        self.my_sender = my_sender
        self.my_user = my_user
        self.password = password
        self.SMTP = SMTP
        self.port = port
        self.server = None
        self.login = None
        self.sendmail = None

    def send_mail(self, message):
        self.server = smtplib.SMTP_SSL(self.SMTP, self.port)
        self.login = self.server.login(self.my_sender, self.password)
        self.sendmail = self.server.sendmail(
            self.my_sender, " ".join(self.my_user).split(" "), message
        )
        self.server.quit()


class Messages:
    def __init__(self, my_sender, my_user, theme, content):
        self.my_sender = my_sender
        self.my_user = my_user
        self.theme = theme
        self.content = content

    def generate_message(self):
        message = MIMEMultipart()
        message.attach(MIMEText(self.content, "html", "utf-8"))
        # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        message["From"] = formataddr(["Sunwoda", self.my_sender])
        # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        message["To"] = formataddr(["Firend", " ".join(self.my_user)])
        # 邮件的主题，也可以说是标题
        message["Subject"] = self.theme
        return message.as_string()