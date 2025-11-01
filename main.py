# Author Loik Andrey 7034@balancedv.ru
import config
import pandas as pd
import smbclient
from loguru import logger
import send_mail
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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

    # Используем для тестов в случае возникновения ошибок
    problematic_rows = find_problematic_rows(df_result)
    print(f"Проблемные строки: {problematic_rows}")

    # Меняем формат Даты
    df_result[0] = pd.to_datetime(df_result[0], format="%d.%m.%Y %H:%M:%S")
    df_result[0] = df_result[0].dt.floor('D')

    # Удаляем дубликаты по первой и второй колонке
    df_result = df_result.drop_duplicates(subset=[0, 1, 4])

    # Переименовываем колонки
    df_result.columns = ['Дата', 'ИК сотрудника', 'Код источник', 'Код добавленный', 'Номер группы']
    return df_result


def find_problematic_rows(df_result):
    """
    Проверка на ошибки при преобразовании первой колонки в дату
    """
    problematic_indices = []

    for idx, value in enumerate(df_result[0]):
        try:
            pd.to_datetime(value, format="%d.%m.%Y %H:%M:%S")
        except ValueError:
            problematic_indices.append(idx)
            print(f"Ошибка в строке {idx}: '{value}'")

    return problematic_indices


def date_report():
    """
    Получаем дату отчета
    :return: -> type datetime - дата отчёта
    """
    return datetime.today() - timedelta(3)


def filter_df_by_date(df, date_filter: datetime, month_report=False, year_report=False):
    """
    Оставляем в DataFrame данные больше значения date_filter

    :param year_report: bool, Передайте True, если нужен фильтр за год указанный в date_filter
    :param month_report: bool, Передайте True, если нужен фильтр за месяц указанный в date_filter
    :param date_filter: datetime, дата фильтрации
    :param df: DataFrame с данными за весь период

    :return: DataFrame с данными за требуемый период
    """
    # Определяем дату начала и окончания для фильтра
    date_filter_start, date_filter_end, period = None, None, None
    if year_report:
        date_filter_start = datetime(date_filter.year, 1, 1)
        date_filter_end = datetime(date_filter.year, 1, 1) + relativedelta(years=1)
    elif month_report:
        date_filter_start = datetime(date_filter.year, date_filter.month, 1)
        date_filter_end = datetime(date_filter.year, date_filter.month, 1) + relativedelta(months=1)
    else:
        exit()
    # logger.info(f"Оставляем в отчете данные за период: {period}")
    df = df.loc[df['Дата'] >= date_filter_start]
    df = df.loc[df['Дата'] < date_filter_end]
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

            logger.info("Оставляем данные за год отчета")
            df_cross = filter_df_by_date(df_cross, date_report(), year_report=True)

            logger.info("Добавляем данные в общий DatFrame по всем типам связи")
            df_cross_total = pd.concat([df_cross_total, df_cross])

            logger.info("Сохраняем данные в словарь")
            if item.find('Аналог (Автомат)') != -1:
                dict_df['А'] = df_cross
            elif item.find('Новый номер (Автомат)') != -1:
                dict_df['Н'] = df_cross
            else:
                dict_df['М'] = df_cross

    logger.info("Добавляем общие данные в словарь")
    dict_df['T'] = df_cross_total

    return dict_df


def report_to_excel(df_dict):
    """
    Сохраняем отчёт в эксель
    :param df_dict:
    :return: list -> Имя файла с итоговым отчётом
    """
    date_report_name = date_report()
    file_name = f"Связи кроссов на {date_report_name.strftime('%Y-%m')}.xlsx"

    # Открываем файл для записи
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        workbook = writer.book  # Открываем книгу для записи
        logger.info('Добавляем в файл отчеты по месяцам')
        months_reports(writer, workbook, df_dict)

        logger.info('Добавляем отчет колонку с Полугодием')
        df_year = set_period(df=df_dict['T'])

        logger.info('Записываем данные по Полугодиям')
        total_result_to_xlsx(writer, workbook, data_pt=df_year)

        logger.info('Добавляем отчет за год, если месяц отчета Декабрь')
        if date_report_name.month == 12:
            year_result_to_xlsx(writer, workbook, data_pt=df_year)
    return [file_name]


def months_reports(writer, workbook, df_dict):
    date = date_report()
    cur_month = date.month
    month = 1
    while month <= cur_month:
        sheet_name_month_report = str(date.year)[-2:] + '-' + str(month)
        wks1 = workbook.add_worksheet(sheet_name_month_report)

        # logger.info("Оставляем данные по общему количеству связей за записываемый месяц")
        df_cross_month_total = filter_df_by_date(df_dict['T'], datetime(date.year, month, 1), month_report=True)
        df_count_month_total = count_add_cross(df_cross_month_total)

        # Определяем кол-во строк первого графика
        skip_row_after_total = len(df_count_month_total)

        # Начальные данные колонок и столбцов для записи
        row_total, start_col, start_row, table_width = 4, 1, 1, 3
        start_row_type = row_total + skip_row_after_total + 6

        for key, val in df_dict.items():
            # logger.info("Оставляем данные за месяц отчета по типу связей за записываемый месяц")
            df_cross = filter_df_by_date(val, datetime(date.year, month, 1), month_report=True)
            df_cross_count = count_add_cross(df_cross)
            header = ''
            if key == 'А':
                header = 'Аналоги 100%'
                start_col = 1
                start_row = start_row_type
            elif key == 'Н':
                header = 'Новый номер'
                start_col = 1 + 2 * table_width
                start_row = start_row_type
            elif key == 'М':
                header = 'МОС'
                start_col = 1 + table_width
                start_row = start_row_type
            elif key == 'T':
                header = 'Общее кол-во связей за месяц'
                start_col = 1
                start_row = row_total
                df_cross_count = df_count_month_total
            df_cross_count.to_excel(writer, sheet_name=sheet_name_month_report, startcol=start_col,
                                    startrow=start_row, index=False, header=False)
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

            # Записываем заголовок таблицы
            wks1.write(start_row - 3, start_col, header, format_header)
            # Записываем заголовок колонок
            wks1.write_row(start_row - 1, start_col, df_cross_count.columns, format_bold)
            # Формат колонок
            wks1.set_column(start_col, start_col + 1, 20, None)
        month += 1
    return


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
    year = date_report().year

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


def total_result_to_xlsx(writer, workbook, data_pt):
    """
    Переработка DataFrame и запись в эксель данных
    :param data_pt: DataFrame для записи
    :param workbook: Книга эксель для записи
    :param writer:
    :return: передача записи дальше
    """
    # Получаем словари форматов для эксель
    year_format, caption_format, sales_type_format, month_format, sum_format, quantity_format = format_custom(workbook)
    # Получаем количество записей по полугодиям.
    data_pt1 = data_pt.groupby(['Период']).count()['Номер группы']

    # Получаем количество записей по сотрудникам в каждом полугодии
    data_pt2 = data_pt.groupby(['Период', 'ИК сотрудника']).count()['Номер группы']
    wks1 = None

    for i in data_pt1.index.unique(level=0):  # Цикл по периодам
        start_row = 4  # Задаём первую строку для записи таблицы с данными
        start_col = 0  # Задаём первую колонку
        sheet_name = i
        if i == 'I Полугодие':
            months = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь']
        else:
            months = ['Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
        # Записываем данные по каждому периоду в эксель
        employee = data_pt2.loc[[i]].sort_values(ascending=False)  # Сортируем кол-во записей по убыванию по сотрудникам

        for k in employee.index.unique(level=1):  # Цикл по сотрудникам
            start_col = 3
            # Записываем данные по каждому ИК сотрудника по каждому периоду в эксель
            employee.loc[[(i, k)]].droplevel(level=0).to_excel(writer, sheet_name=sheet_name, startrow=start_row,
                                                               startcol=1, header=False)
            wks1 = writer.sheets[sheet_name]
            # Поскольку формат индекса изменить нельзя, то перезаписываем наименование каждого
            # ИК сотрудника и меняем формат
            wks1.write(f'B{start_row + 1}', k, sales_type_format)
            # Изменяем формат всей строки для каждого сотрудника с данными о количестве
            wks1.conditional_format(f'B{start_row + 1}:C{start_row + 1}',
                                    {'type': 'no_errors', 'format': sales_type_format})

            # Выбираем данные по каждому полугодию и сотруднику и сортируем по дате
            data_pt4 = data_pt.loc[(data_pt['Период'] == i) & (data_pt['ИК сотрудника'] == k)]
            employee_month = data_pt4.groupby(['Месяц']).count()['Номер группы']

            for month in months:
                try:
                    val = employee_month[month]
                except KeyError:
                    val = None

                wks1.write(start_row, start_col, val, quantity_format)
                start_col += 1
            start_row += 1  # Изменяем значение стартовой строки для следующих записей

        # Запись и формат заголовка таблицы
        wks1.write('B2', f'Общее количество связей за {i}', caption_format)
        # Запись и формат заголовка колонок таблицы
        wks1.write('B4', 'ИК Сотрудника', year_format)
        wks1.write('C4', i, year_format)
        for ind, m in enumerate(months):
            wks1.write(3, ind + 3, m, year_format)
            wks1.set_column(ind + 3, ind + 3, 10, None)
        wks1.set_column('B:B', 16, None)  # Изменяем ширину первой колонки, где расположен Год, Тип продажи и месяц
        wks1.set_column('C:C', 14, None)  # Изменяем ширину и формат колонки с количеством строк

        wks1.autofilter(3, 1, start_row, start_col-1)  # Добавляем фильтр в отчет

        # Добавление отображение итогов группировок сверху
        wks1.outline_settings(True, False, False, False)
    wks1.activate()
    return


def year_result_to_xlsx(writer, workbook, data_pt):
    """
    Переработка DataFrame и запись в эксель данных
    :param data_pt: DataFrame для записи
    :param workbook: Книга эксель для записи
    :param writer: Писатель
    :return: передача записи дальше
    """
    # Получаем словари форматов для эксель
    year_format, caption_format, sales_type_format, month_format, sum_format, quantity_format = format_custom(workbook)

    # Получаем количество записей по сотрудникам за год
    data_pt2 = data_pt.groupby(['ИК сотрудника']).count()['Номер группы']
    months = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
              'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    sheet_name = str(date_report().year)
    # Записываем данные по году в эксель
    start_row = 4
    employee = data_pt2.sort_values(ascending=False)  # Сортируем кол-во записей по убыванию по сотрудникам
    for k in employee.index.unique(level=0):  # Цикл по сотрудникам
        start_col = 3
        # Записываем данные по каждому ИК сотрудника по каждому периоду в эксель
        employee.loc[[k]].to_excel(writer, sheet_name=sheet_name, startrow=start_row, startcol=1, header=False)
        wks1 = writer.sheets[sheet_name]
        # Поскольку формат индекса изменить нельзя, то перезаписываем наименование каждого
        # ИК сотрудника и меняем формат
        wks1.write(f'B{start_row + 1}', k, sales_type_format)
        # Изменяем формат всей строки для каждого сотрудника с данными о количестве
        wks1.conditional_format(f'B{start_row + 1}:C{start_row + 1}',
                                {'type': 'no_errors', 'format': sales_type_format})

        # Выбираем данные по каждому полугодию и сотруднику и сортируем по дате
        data_pt4 = data_pt.loc[(data_pt['ИК сотрудника'] == k)]
        employee_month = data_pt4.groupby(['Месяц']).count()['Номер группы']

        for month in months:
            try:
                val = employee_month[month]
            except KeyError:
                val = None

            wks1.write(start_row, start_col, val, quantity_format)
            start_col += 1
        start_row += 1  # Изменяем значение стартовой строки для следующих записей

        # Запись и формат заголовка таблицы
        wks1.write('B2', f'Общее количество связей за {sheet_name} год', caption_format)
        # Запись и формат заголовка колонок таблицы
        wks1.write('B4', 'ИК Сотрудника', year_format)
        wks1.write('C4', sheet_name, year_format)
        for ind, m in enumerate(months):
            wks1.write(3, ind + 3, m, year_format)
        wks1.set_column('B:B', 16, None)  # Изменяем ширину первой колонки, где расположен Год, Тип продажи и месяц
        wks1.set_column('C:C', 8, None)  # Изменяем ширину и формат колонки с количеством строк
        wks1.autofilter('B4:O4')  # Добавляем фильтр в отчет

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


def run_old():
    # Получаем данные в виде словаря
    df_dict = get_report_cross()
    # Записываем полученный данные в эксель
    file = report_to_excel(df_dict)

    logger.info('Подготавливаем отчет за год')
    df_year = set_period(df=df_dict['T_Y'])
    # file += year_result_to_xlsx(,
    logger.info(file)

    logger.info('Отправляем файл на почту')
    # send_file_to_mail(file)
    logger.info('Программа завершила работу')


def run():
    # Получаем данные в виде словаря
    df_dict = get_report_cross()
    # Записываем полученный данные в эксель
    file = report_to_excel(df_dict)
    logger.info(file)

    logger.info('Отправляем файл на почту')
    send_file_to_mail(file)
    logger.info('Программа завершила работу')


if __name__ == '__main__':
    run()
