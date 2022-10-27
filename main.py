# Author Loik Andrey 7034@balancedv.ru
import config
import pandas as pd
import smbclient
from loguru import logger
import send_mail
from datetime import datetime, timedelta

"""
Заготовка для записи логов в файл
logger.add(config.FILE_NAME_CONFIG,
           format="{time:DD/MM/YY HH:mm:ss} - {file} - {level} - {message}",
           level="INFO",
           rotation="1 month",
           compression="zip")
"""


def read_report(path_file):
    """
    Считываем файл в DataFrame
    :param path_file: -> str - Путь к файлу
    :return: DataFrame с данными из файла
    """
    try:
        with smbclient.open_file(path_file, mode="rb") as fd:
            file_bytes = fd.read()
            return pd.read_excel(file_bytes)
    except ConnectionError:
        logger.error(f"Не могу подключиться к папке с отчетами:")
        logger.error(ConnectionError)


def rebuild_df(df):
    """
    Очищаем DataFrame и подготавливаем к дальнейшей работе
    :param df: DataFrame с кроссами из экселя
    :return: Очищенный DataFrame
    """
    mask_start = df == 'Код'
    start_row = df[mask_start].dropna(axis=0, how='all').index.values[0]
    df.columns = df.iloc[start_row]
    df = df[start_row + 1:]
    df = df.dropna(axis=1, how='all').set_index('Код')
    return df


def split_df(df):
    """
    Разделяем на отдельные колонки данные из колонки "Дополнительная информация"
    :param df: DataFrame с колонкой "Дополнительная информация"
    :return: DataFrame с колонками из колонки "Дополнительная информация"
    """
    # Разбиваем на колонки по разделителю - ";"
    df_temp1 = df['Дополнительная информация'].str.split(';', expand=True)

    # Объединяем все колонки в одну
    df_temp = pd.DataFrame()
    for i in df_temp1.columns:
        # Берём каждую колонку по порядку без пустых значений
        df_temp2 = df_temp1[[i]].dropna(how='all', axis=0).copy()
        # Изменяем наименование колонки как у первой
        df_temp2.columns = [0]
        # Объединяем в один DataFrame
        df_temp = pd.concat([df_temp, df_temp2])

    # Разделяем на колонки по разделителю - "/"
    df_result = df_temp[0].str.split('/', expand=True)
    # Удаляем дубликаты по первой и второй колонке
    df_result = df_result.drop_duplicates(subset=[0, 1])
    # Переименовываем колонки и меняем формат даты
    df_result.columns = ['Дата', 'ИК сотрудника', 'Код источник', 'Код добавленный', 'Номер группы']
    df_result['Дата'] = pd.to_datetime(df_result['Дата'], format="%d.%m.%Y %H:%M:%S")

    # Оставляем данные за отчётный период
    df_result = sort_month_report(df_result)

    return df_result


def month_report():
    """
    Получаем дату отчета
    :return: -> str - в виде 'ГГГГ-ММ'
    """
    return (datetime.today() - timedelta(3)).strftime('%Y-%m')


def sort_month_report(df):
    """
    Оставляем в DataFrame данные только за месяц отчета

    :param df: DataFrame с данными за весь период

    :return: DataFrame с данными за месяц отчета
    """
    date_report = month_report()
    logger.info(f'Оставляем в отчете данные за период: {date_report}')
    df = df.query(f"Дата > '{date_report}'")
    return df


def count_add_cross(df):
    """
    Считаем количество изменений в связях по ИК-сотрудника

    :param df: DataFrame с уникальными значениями создания связей

    :return: DataFrame - Количество уникальных связей и ИК-сотрудника
    """
    df = df.groupby('ИК сотрудника', as_index=False).count()
    sort_df = df.sort_values('Номер группы', ascending=False)[['ИК сотрудника', 'Номер группы']]
    sort_df.rename(columns={'Номер группы': 'Кол-во связей'}, inplace=True)
    return sort_df


def get_report_cross():
    """
    Считываем файлы со связями из папки в локальной сети

    :return: dict -> c очищенными данными и количеством связей
    """
    # Получаем список файлов на сервере
    smbclient.ClientConfig(username=config.LOCAL_PATH['USER'], password=config.LOCAL_PATH['PSW'])
    path = config.LOCAL_PATH['PATH']
    list_file = []
    try:
        list_file = smbclient.listdir(path)
        logger.info(f"Получили список файлов с отчётами: {list_file}")
    except ConnectionError:
        logger.error(f"Не могу подключиться к папке с отчетами:")
        logger.error(ConnectionError)

    # Обрабатываем файлы с отчетами b сохраняем в словарь
    dict_df = dict()
    for item in list_file:
        if item.endswith('.xlsx'):
            path_file = path + "\\" + item

            logger.info(f"Считываем файл: '{item}' с локального сервера")
            df_cross = read_report(path_file)

            logger.info("Парсим колонку 'Дополнительная информация'")
            df_cross = rebuild_df(df_cross)  # Очищаем DataFrame
            df_cross = split_df(df_cross)  # Разделяем по колонкам

            logger.info("Считаем количество связей по менеджерам")
            df_count = count_add_cross(df_cross)
            logger.info("Сохраняем данные в словарь")
            if item.find('Аналог (Автомат)') != -1:
                dict_df['А'] = df_cross
                dict_df['А_count'] = df_count
            elif item.find('Новый номер (Автомат)') != -1:
                dict_df['Н'] = df_cross
                dict_df['Н_count'] = df_count
            else:
                dict_df['М'] = df_cross
                dict_df['М_count'] = df_count
    return dict_df


def write_data(writer, sheet_name, df):
    """
    Записываем общие данные о связях на отдельные вкладки
    :param df: DataFrame  с общими данными
    :param writer: объект записи
    :param sheet_name: наименование вкладки
    """
    df.to_excel(writer, sheet_name=sheet_name)
    return


def report_to_excel(df_dict):
    """
    Сохраняем отчёт в эксель
    :param df_dict:
    :return: str -> Имя файла с итоговым отчётом
    """
    date_report = month_report()
    file_name = f'Связи кроссов за {date_report}.xlsx'

    # Открываем файл для записи
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        workbook = writer.book  # Открываем книгу для записи
        sheet_name = date_report
        wks1 = workbook.add_worksheet(sheet_name)
        start_col = 1
        start_row = 18

        for key, val in df_dict.items():
            # Определяем наименование вкладки для сводной таблицы
            logger.info(f'Записываем {key=}')
            if key in ['А', 'Н', 'М']:
                sheet_name1 = f'Данные {key}'
                write_data(writer, sheet_name1, val)
            else:
                format_bold = workbook.add_format({
                    'font_name': 'Arial',
                    'font_size': '10',
                    'align': 'left',
                    'bold': True,
                    'bg_color': '#F4ECC5',
                    'border': True,
                    'border_color': '#CCC085'
                })

                val.to_excel(writer, sheet_name=sheet_name, startcol=start_col,
                             startrow=start_row, index=False, header=False)
                wks1.write_row(start_row-1, start_col, val.columns, format_bold)  # сразу пишем целую строку данных

                # Формат колонок
                wks1.set_column(start_col, start_col+1, 20, None)

                # Запись графиков
                end_row = start_row + len(val)-1
                chart = workbook.add_chart({'type': 'column'})
                chart.add_series({
                    'categories': [f'{date_report}', start_row, start_col, end_row, start_col],
                    'values': [f'{date_report}', start_row, start_col+1, end_row, start_col+1],
                    'data_labels': {'series_name': False},
                })
                if key == 'А_count':
                    title_plt = 'Аналоги 100%'
                elif key == 'Н_count':
                    title_plt = 'Новый номер'
                elif key == 'М_count':
                    title_plt = 'МОС'
                chart.set_title({'name': title_plt})
                chart.set_legend({'none': True})

                wks1.insert_chart(1, start_col-1, chart)
                start_col += 5
        wks1.set_first_sheet()
        wks1.activate()
    return file_name


def send_file_to_mail(filename):
    """
    Отправляем файл на почту
    :param filename: -> str - Имя файла для отправки
    :return:
    """
    message = {
        'Subject': f"Отчёт {filename[:-5]}",
        'email_content': f"Сформирован отчёт: {filename[:-5]}",
        'To': config.TO_EMAILS['TO_CORRECT'],
        'File_name': filename,
        'Temp_file': filename
    }
    send_mail.send(message)


def run():
    # Получаем данные в виде словаря
    df_dict = get_report_cross()
    # Записываем полученный данные в эксель
    file = report_to_excel(df_dict)
    logger.info('Отправляем файл на почту')
    send_file_to_mail(file)
    logger.info('Программа завершила работу')


if __name__ == '__main__':
    run()
