from config import EMAIL_CONFIG
from loguru import logger
import smtplib  # Импортируем библиотеку по работе с SMTP

# Добавляем необходимые подклассы - MIME-типы
from email.mime.multipart import MIMEMultipart  # Многокомпонентный объект
from email.mime.text import MIMEText  # Текст/HTML
from email.mime.base import MIMEBase  # Общий тип
from email import encoders  # Импортируем энкодер


def send(message=None):
    """
    Отправляем письмо

    :param message:
        {
        'Subject': str - Тема письма,
        'email_content': str - Текст письма,
        'To': list - Список с адресами получателей,
        'File_name': str - Наименование файла, которое будет отображаться в письме,
        'Temp_file': str - Наименование файла, которое будет добавлено к письму,
        }
    :return:
    """
    if message is None:
        logger.error("No options to send email. Pass all parameters to the function: 'message'")
    else:
        addr_from = EMAIL_CONFIG['FROM']  # Отправитель
        addr_to = message['To']  # Получатель
        password = EMAIL_CONFIG['PSW']  # Пароль

        logger.info(f"Send message to emails {addr_to}")

        msg = MIMEMultipart()  # Создаем сообщение
        msg['From'] = addr_from # Адресат
        msg['To'] = ','.join(addr_to)  # Получатель
        msg['Subject'] = message['Subject']  # Тема сообщения

        # Текст сообщения в формате html
        email_content = message['email_content']

        msg.attach(MIMEText(email_content, 'html'))  # Добавляем в сообщение html

        # Присоединяем файл к письму
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(message['Temp_file'], "rb").read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=('utf-8', 'fr', message['File_name']))
        msg.attach(part)

        server = smtplib.SMTP_SSL('smtp.yandex.ru', 465)  # Создаем объект SMTP
        server.login(addr_from, password)  # Получаем доступ
        server.send_message(msg)  # Отправляем сообщение
        server.quit()  # Выходим

        logger.info("Mail sending completed")
    return

