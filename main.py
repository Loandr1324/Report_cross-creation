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
    return df_result


def month_report():
    """
    Получаем дату отчета
    :return: -> str - в виде 'ГГГГ-ММ'
    """
    return (datetime.today() - timedelta(3)).strftime('%Y-%m')


def filter_df_by_date(df, date_filter: str):
    """
    Оставляем в DataFrame данные больше значения date_filter

    :param date_filter: str, дата фильтрации в виде YYYY-mm или YYYY
    :param df: DataFrame с данными за весь период

    :return: DataFrame с данными за месяц отчета
    """
    date_report = date_filter
    logger.info(f'Оставляем в отчете данные за период: {date_report}')
    df = df.query(f"Дата >= '{date_report}'")
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

    # Обрабатываем файлы с отчетами и сохраняем в словарь
    dict_df = dict()
    df_cross_total = pd.DataFrame()
    for item in list_file:
        if item.endswith('.xlsx'):
            path_file = path + "\\" + item

            logger.info(f"Считываем файл: '{item}' с локального сервера")
            df_cross = read_report(path_file)

            logger.info("Парсим колонку 'Дополнительная информация'")
            df_cross = rebuild_df(df_cross)  # Очищаем DataFrame
            df_cross = split_df(df_cross)  # Разделяем по колонкам

            logger.info("Добавляем данные в общий DatFrame")
            df_cross_total = pd.concat([df_cross_total, df_cross])

            logger.info("Оставляем данные только за предыдущий месяц")
            df_cross = filter_df_by_date(df_cross, month_report())

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

    logger.info("Оставляем данные в общей таблице только за последний месяц:")
    df_cross_month_total = filter_df_by_date(df_cross_total, month_report())

    logger.info("Считаем количество связей по менеджерам")
    df_count_month_total = count_add_cross(df_cross_month_total)
    dict_df['T_M'] = df_cross_month_total
    dict_df['T_M_count'] = df_count_month_total

    logger.info("Оставляем данные в общей таблице только за последний год:")
    df_cross_year_total = filter_df_by_date(df_cross_total, month_report()[:-3])
    dict_df['T_Y'] = df_cross_year_total

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
    :return: list -> Имя файла с итоговым отчётом
    """
    date_report = month_report()
    file_name = f'Связи кроссов за {date_report}.xlsx'

    # Открываем файл для записи
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        workbook = writer.book  # Открываем книгу для записи
        sheet_name = date_report
        wks1 = workbook.add_worksheet(sheet_name)

        # Формат заголовка таблицы
        format_header = workbook.add_format({
            'font_name': 'Arial',
            'font_size': '14',
            'align': 'left',
            'bold': True
        })

        # Формат заголовка столбцов
        format_bold = workbook.add_format({
            'font_name': 'Arial',
            'font_size': '10',
            'align': 'left',
            'bold': True,
            'bg_color': '#F4ECC5',
            'border': True,
            'border_color': '#CCC085'
        })

        skip_row_after_total = len(df_dict['T_M_count'])
        row_total = 4
        col_total = 1
        start_col = 1
        start_row = row_total + skip_row_after_total + 6

        for key, val in df_dict.items():
            # Определяем наименование вкладки для сводной таблицы
            logger.info(f'Записываем {key=}')
            if key == 'T_M_count':

                val.to_excel(writer, sheet_name=sheet_name, startcol=col_total,
                             startrow=row_total, index=False, header=False)
                # Записываем заголовок таблицы
                wks1.write(row_total - 3, col_total, 'Общее кол-во связей за месяц', format_header)
                # Записываем заголовок колонок
                wks1.write_row(row_total - 1, col_total, val.columns, format_bold)

            elif key in ['А_count', 'Н_count', 'М_count']:
                if key == 'А_count':
                    header = 'Аналоги 100%'
                elif key == 'Н_count':
                    header = 'Новый номер'
                elif key == 'М_count':
                    header = 'МОС'

                val.to_excel(writer, sheet_name=sheet_name, startcol=start_col,
                             startrow=start_row, index=False, header=False)

                # Записываем заголовок таблицы
                wks1.write(start_row - 3, start_col, header, format_header)
                # Записываем заголовок колонок
                wks1.write_row(start_row - 1, start_col, val.columns, format_bold)  # сразу пишем целую строку данных

                # Формат колонок
                wks1.set_column(start_col, start_col + 1, 20, None)

                # Меняем номер колонки для следующей таблицы
                start_col += 3

        wks1.set_first_sheet()
        wks1.activate()
    return [file_name]


def send_file_to_mail(files):
    """
    Отправляем файл на почту
    :param files: -> str - Имя файла для отправки
    :return:
    """
    message = {
        'Subject': f"Отчёт {files[0][:-5]}",
        'email_content': f"Сформирован отчёт: {files[0][:-5]}",
        'To': config.TO_EMAILS['TO_CORRECT'],
        'File_name': files,
        'Temp_file': files
    }
    send_mail.send(message)


def set_period(df):
    """Добавляем колонку Период с номером полугодия и Месяц на основании данных из колонки дата"""
    # Получаем год отчёта
    year = month_report()[:-3]

    # Заполняем колонку период полугодием
    df['Период'] = 'I Полугодие'
    mask = df['Дата'] >= datetime.strptime(f'{year}-07', '%Y-%m')
    df.loc[mask, 'Период'] = 'II Полугодие'

    # Заполняем колонку месяц
    df['Месяц'] = 'не определено'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-01', '%Y-%m'), 'Месяц'] = 'Январь'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-02', '%Y-%m'), 'Месяц'] = 'Февраль'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-03', '%Y-%m'), 'Месяц'] = 'Март'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-04', '%Y-%m'), 'Месяц'] = 'Апрель'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-05', '%Y-%m'), 'Месяц'] = 'Май'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-06', '%Y-%m'), 'Месяц'] = 'Июнь'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-07', '%Y-%m'), 'Месяц'] = 'Июль'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-08', '%Y-%m'), 'Месяц'] = 'Август'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-09', '%Y-%m'), 'Месяц'] = 'Сентябрь'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-10', '%Y-%m'), 'Месяц'] = 'Октябрь'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-11', '%Y-%m'), 'Месяц'] = 'Ноябрь'
    df.loc[df['Дата'] >= datetime.strptime(f'{year}-12', '%Y-%m'), 'Месяц'] = 'Декабрь'
    return df


def total_result_to_xlsx(df=None):
    """
    Запись результата в эксель
    :param df: DataFame с данными по количеству связей
    :return: None
    """
    exel_file = f"Связи кроссов за {month_report()[:-3]} год.xlsx"
    sheet_name = f"Итого за {month_report()[:-3]} год"  # Наименование вкладки для сводной таблицы
    with pd.ExcelWriter(exel_file, engine='xlsxwriter') as writer:  # Открываем файл для записи
        workbook = writer.book
        # Записываем данные на вкладку данные
        df_write_xlsx(writer, sheet_name, workbook, df)
    return [exel_file]


def df_write_xlsx(writer, sheet_name: str, workbook, data_pt):
    """
    Переработка DataFrame и запись в эксель данных
    :param data_pt: DataFrame для записи
    :param workbook: Книга эксель для записи
    :param sheet_name: Наименование вкладки
    :param writer:
    :return: передача записи дальше
    """
    # Получаем словари форматов для эксель
    year_format, caption_format, sales_type_format, month_format, sum_format, quantity_format = format_custom(workbook)
    # Получаем количество записей по полугодиям.
    data_pt1 = data_pt.groupby(['Период']).count()['Номер группы']
    # Получаем количество записей по сотрудникам в каждом полугодии
    data_pt2 = data_pt.groupby(['Период', 'ИК сотрудника']).count()['Номер группы']

    start_row = 4  # Задаём первую строку для записи таблицы с данными
    for i in data_pt1.index.unique(level=0):  # Цикл по периодам
        period = data_pt1.loc[[i]]  # Создаём DataFrame по каждому периоду для записи
        # Записываем данные по каждому периоду в эксель
        period.to_excel(writer, sheet_name=sheet_name, startrow=start_row, startcol=1, header=False)

        wks1 = writer.sheets[sheet_name]  # Открываем вкладку для форматирования
        wks1.set_column('B:B', 12, None)  # Изменяем ширину первой колонки, где расположен Год, Тип продажи и месяц
        wks1.set_column('C:C', 7, quantity_format)  # Изменяем ширину и формат колонки с количеством строк

        # Поскольку формат индекса изменить нельзя, то перезаписываем наименование каждого года и меняем формат
        wks1.write(f'B{start_row + 1}', i, year_format)
        # Изменяем формат всей строки для каждого периода с данными о количестве
        wks1.conditional_format(f'B{start_row + 1}:C{start_row + 1}',
                                {'type': 'no_errors', 'format': year_format})
        wks1.set_row(start_row, 20, None)  # Изменяем высоту каждой строки с периодом
        start_row += len(data_pt1.loc[[i]])  # Изменяем значение стартовой строки для следующих записей
        employee = data_pt2.loc[[i]].sort_values(ascending=False)  # Сортируем кол-во записей по убыванию по сотрудникам
        for k in employee.index.unique(level=1):  # Цикл по сотрудникам
            # Записываем данные по каждому ИК сотрудника по каждому периоду в эксель
            employee.loc[[(i, k)]].droplevel(level=0).to_excel(writer, sheet_name=sheet_name, startrow=start_row,
                                                               startcol=1, header=False)
            # Добавляем группировку по периоду, данные по месяцам не скрываем
            wks1.set_row(start_row, None, None, {'level': 1})
            # Поскольку формат индекса изменить нельзя, то перезаписываем наименование каждого
            # ИК сотрудника и меняем формат
            wks1.write(f'B{start_row + 1}', k, sales_type_format)
            # Изменяем формат всей строки для каждого сотрудника с данными о количестве
            wks1.conditional_format(f'B{start_row + 1}:C{start_row + 1}',
                                    {'type': 'no_errors', 'format': sales_type_format})
            start_row += len(data_pt2.loc[[(i, k)]])  # Изменяем значение стартовой строки для следующих записей

            # Выбираем данные по каждому полугодию и сотруднику и сортируем по дате
            data_pt4 = data_pt.loc[(data_pt['Период'] == i) & (data_pt['ИК сотрудника'] == k)].sort_values('Дата')

            # Записываем значения по каждому месяцу в эксель
            for m in data_pt4['Месяц'].unique():
                employee_month = data_pt4.loc[data_pt4['Месяц'] == m].groupby(['Месяц']).count()['Номер группы']
                for ind, val in employee_month.items():
                    # Записываем данные по месяцам в таблицу
                    wks1.write(f'B{start_row + 1}', ind, month_format)
                    wks1.write(f'C{start_row + 1}', val, quantity_format)
                    wks1.set_row(start_row, None, None, {'level': 2, 'hidden': True})  # Добавляем строку в группировку
                    start_row += 1  # Изменяем значение стартовой строки для следующих записей

        # Запись и формат заголовка таблицы
        wks1.write('B2', 'Общее количество связей по полугодиям', caption_format)
        # Добавление отображение итогов группировок сверху
        wks1.outline_settings(True, False, False, False)
    return


def format_custom(workbook):
    year_format = workbook.add_format({
        'font_name': 'Arial',
        'font_size': '10',
        'align': 'left',
        'bold': True,
        'bg_color': '#F4ECC5',
        'border': True,
        'border_color': '#CCC085'
    })
    sales_type_format = workbook.add_format({
        'font_name': 'Arial',
        'font_size': '8',
        'align': 'left',
        'border': True,
        'border_color': '#CCC085',
        'bg_color': '#F8F2D8'
    })
    month_format = workbook.add_format({
        'font_name': 'Arial',
        'font_size': '8',
        'align': 'right',
        'bold': False,
        'border': True,
        'border_color': '#CCC085'
    })
    sum_format = workbook.add_format({
        'num_format': '# ### ##0.00"р.";[red]-# ##0.00"р."',
        'font_name': 'Arial',
        'font_size': '8',
        'border': True,
        'border_color': '#CCC085'
    })
    quantity_format = workbook.add_format({
        'num_format': '# ### ##0',
        'font_name': 'Arial',
        'font_size': '8',
        'border': True,
        'border_color': '#CCC085'
    })
    caption_format = workbook.add_format({
        'font_name': 'Arial',
        'font_size': '14',
        'bold': True,
        'border': True,
        'border_color': '#CCC085'
    })

    return year_format, caption_format, sales_type_format, month_format, sum_format, quantity_format


def run():
    # Получаем данные в виде словаря
    df_dict = get_report_cross()
    # Записываем полученный данные в эксель
    file = report_to_excel(df_dict)

    logger.info('Подготавливаем отчет за год')
    df_year = set_period(df=df_dict['T_Y'])
    file += total_result_to_xlsx(df=df_year)
    logger.info(file)

    logger.info('Отправляем файл на почту')
    send_file_to_mail(file)
    logger.info('Программа завершила работу')


if __name__ == '__main__':
    run()
