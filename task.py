"""
class task
12.02.2021
"""
import logging
import threading
import time
import traceback
import snap7
import telebot
import socket
from snap7.util import *
from time import sleep
from base64 import urlsafe_b64encode, urlsafe_b64decode

from config import *
from sql import RobotSQL, MovieSQL
from moviemaker import MovieMaker
import uuid
from config import cameras


class ThreadPLC(threading.Thread):
    def __init__(self, plc=()):
        threading.Thread.__init__(self, args=(), name=plc['ip'], kwargs=None)
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format="[%(asctime)s: %(filename)s:%(lineno)s  - %(funcName)20s() ] %(message)s",
                            level=logging.WARNING, datefmt='%Y-%m-%d %H:%M:%S')

        self.plc_client = snap7.client.Client()
        self.plc_info = plc
        self.sql = RobotSQL()
        self.robots = self.sql.create_robot_name(self.plc_info['name'], self.plc_info['robot_types'],
                                                 self.plc_info['n_robots'])
        self.telegram_bot = telebot.TeleBot(alarm_robot_token)
        self.tasks = [None, Task(self, 1), Task(self, 2)]

    def run(self):
        self.logger.info(f"ThreadPLC {self.plc_info['name']}: {self.plc_info['ip']} - started")
        rob_state = [dict(), dict(busy=False, task=dict()), dict(busy=False, task=dict())]
        plc_connected = False

        while True:
            # Формируем задания
            for robot in self.robots:
                task = self.sql.get_free_task(robot)
                # Одиночный Робот
                if robot[-1] == 'a' or robot[-1] == 'k':
                    if task:
                        rob_state[1]['busy'] = True
                        rob_state[1]['task'] = task
                    else:
                        rob_state[1]['busy'] = False
                        rob_state[1]['task'] = dict()
                    rob_state[2]['busy'] = False
                    rob_state[2]['task'] = dict()
                # Спарка Роботов
                else:
                    if task:
                        rob_state[int(robot[-1])]['busy'] = True
                        rob_state[int(robot[-1])]['task'] = task
                    else:
                        rob_state[int(robot[-1])]['busy'] = False
                        rob_state[int(robot[-1])]['task'] = dict()
            # Подключение контроллера
            if not plc_connected:
                try:
                    self.plc_client.connect(self.plc_info['ip'], 0, 1)
                    plc_connected = True
                except Exception as error:
                    self.logger.error(
                        f"PLC {self.plc_info['name']}: {self.plc_info['ip']} - is not reachable. Error {str(error)}")
                    self.plc_client.disconnect()
                    plc_connected = False
            else:
                # Работа с PLC
                for rob_num in range(1, 3):
                    try:
                        self.tasks[rob_num].data_read()
                        if rob_state[rob_num]['task']:
                            self.tasks[rob_num].working(rob_state[rob_num]['task'])
                    except Exception as error:
                        self.logger.error(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())}\n"
                                          f"{self.plc_info['name']}: Не удалось обработать задание:\n"
                                          f"{str(error)} {traceback.format_exc()}")
                        self.plc_client.disconnect()
                        plc_connected = False
            time.sleep(0.1)


class Task:
    def __init__(self, plcthread, rob_num):
        self.dt_start = None  # начало перекладки
        self.dt_finish = None
        self.needprint = False
        self.sheetnumber = 0
        self.wait_list_finish = False  # ожидаем окончания перекладки листа
        self.uuid = None
        self.timeframe = {'start': None, 'finihs': None}
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format="[%(asctime)s: %(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s",
                            level=logging.WARNING, datefmt='%Y-%m-%d %H:%M:%S')
        self.plc_info = plcthread.plc_info  # Конфиг контроллера
        self.plc_client = plcthread.plc_client  # Конфик Подключения контроллера
        self.sql = plcthread.sql  # Конфиг БД
        self.telegram_bot = plcthread.telegram_bot  # Конфиг Телеграма
        self.name = plcthread.name  # Имя контроллера
        self.rob_num = rob_num  # Номер робота
        self.robot_name = plcthread.robots[rob_num - 1]

        self.db_task = 600  # DB Заданий
        self.db_control = 601  # DB Управления элементами системы
        self.db_status = 602  # DB Статусов/Сигнализаций/Информации
        self.db_io = 603  # DB Хранящая значения физических входов/выходов
        self.db_settings = 604  # DB Настроек
        self.db_messages = 605  # DB Телеграм
        self.db_post_height = 607  # DB Высота постов

        self.length_DataTask = 932  # Кол-во байт на одного робота для Заданий
        self.length_DataStatus = 24  # Кол-во байт на одного робота для Статусов
        self.length_DataControl = 26  # Кол-во байт на одного робота для Управления
        self.length_DataIO = 2  # Кол-во байт на одного робота для I/O
        self.length_DataTelegram = 16  # Кол-во байт на одного робота для Телеграм сообщений
        self.length_DataPostHeight = 12  # Кол-во байт на одного робота для Высот поста
        self.length_DataDataExtToServer = 18

        # Data offset
        offset = self.rob_num - 1  # Сдвиг: 0 для первого робота, 1 для второго робота
        self.start_byte_datatask_whole = self.length_DataTask * offset  # Стартовый байт Данных Задания
        self.start_byte_datatask_status = 0  # Стартовый байт Статусных переменных задания
        self.start_byte_datatask_moving = 4  # Стартовый байт Перемещений
        self.start_byte_datatask_sheet = 10  # Стартовый байт Информации о листах
        self.start_byte_datatask_id = 410  # Стартовый байт RobotTaskID

        self.length_datatask_status = 4  # Кол-во байт на одного робота для Заданий - Статус
        self.length_datatask_moving = 6  # Кол-во байт на одного робота для Заданий - Перемещение
        self.length_datatask_sheet = 400  # Кол-во байт на одного робота для Заданий - Информация о листах
        self.length_datatask_id = 512  # Кол-во байт на одного робота для Заданий - ID заданий
        self.length_datastatus_whole = 24  # Кол-во байт на одного робота для Статусов -

        self.start_byte_task = self.length_DataTask * offset  # Стартовый байт Данных Задания
        self.start_byte_status = self.length_DataStatus * offset  # Стартовый байт Данных Статуса
        self.start_byte_telegram = self.length_DataTelegram * offset  # Стартовый байт Данных Телеграм
        self.start_byte_post_height = self.length_DataPostHeight * offset  # Стартовый байт Высот поста
        self.start_byte_control = self.length_DataControl * offset
        self.start_byte_message = self.length_DataDataExtToServer * offset

        self.start_byte_task_status = 0  # Стартовый байт Данных Статуса задания
        self.start_byte_task_moving = 4  # Стартовый байт Данных Задания на перемещение
        self.start_byte_task_id = 10  # Стартовый байт Данных ID задания
        self.start_byte_task_data_sheet = 18  # Стартовый байт Данных Задания информации о листах

        self.byte_zone_from = 0  #
        self.byte_zone_to = 2  #
        self.byte_required = 4  #

        self.old_current_sheet_data = 0

        # Printer call
        self.printer = Printer(self.robot_name, self.plc_client, self.start_byte_control)

    def working(self, task):

        self.task = task  # Задание на перемещение
        # Шаг 0: Принудительное завершение заданий

        # Шаг 1: Отправляем, что OPC получило задание
        if self.task['stage'] == 0:
            if self.task_status == 0 or self.task_status == 3:
                self.sql.configured(self.task['task'])
                self.logger.warning(f"{self.robot_name}: {self.task['task']} - Заданние Сконфигурировано")

        # Шаг 2: Отрпавляем задание в PLC
        if self.task['stage'] == 1:
            if self.task_status == 0 or self.task_status == 3:
                if self.post_task():
                    self.post_start()
            elif self.task_status == 2:
                self.sql.working(self.task['task'])
                self.logger.warning(f"{self.robot_name}: {self.task['task']} - Робот начал Перекладку")

        # Шаг 3: Ждем завершения работы программы
        if self.task['stage'] == 2:
            if self.task_status == 0 or self.task_status == 3:
                self.sql.finish(self.quantity, self.task['task'])
                self.logger.warning(f"{self.robot_name}: {self.task['task']} - Заданние Завершенно")
            elif self.task_status == 2:
                self.wait_finish()

    def post_task(self):
        try:
            # init
            data_moving = bytearray(2)
            data_sheet = bytearray(2)
            data_id = bytearray(512)
            # Data preparation

            task_changed = dict(zone_from=self.change_zone(self.task['zone_from']),
                                zone_to=self.change_zone(self.task['zone_to']))
            set_int(data_moving, 0, task_changed['zone_from'])
            set_int(data_moving, 2, task_changed['zone_to'])
            set_int(data_moving, 4, self.task['required'])
            sheet_task = [256 * x + y for x, y in zip(self.task['cover'], self.task['thickness'])]
            for i in range(self.task['required']):
                set_int(data_sheet, i * 2, sheet_task[i])
            set_string(data_id, 0, str(0), 256)
            set_string(data_id, 256, str(0), 256)
            # Write task in db_task
            self.plc_client.db_write(self.db_task, self.start_byte_datatask_whole + self.start_byte_datatask_moving,
                                     data_moving)
            self.plc_client.db_write(self.db_task, self.start_byte_datatask_whole + self.start_byte_datatask_sheet,
                                     data_sheet)
            self.plc_client.db_write(self.db_task, self.start_byte_datatask_whole + self.start_byte_datatask_id,
                                     data_id)
            # Compare task from robot <====> task from sql
            task_robot = [self.task_robot['zone_from'],
                          self.task_robot['zone_to'],
                          self.task_robot['required'],
                          self.task_robot['cover'],
                          self.task_robot['thickness']]
            task_sql = [task_changed['zone_from'],
                        task_changed['zone_to'],
                        self.task['required'],
                        self.task['cover'],
                        self.task['thickness']]
            if task_sql == task_robot:
                self.logger.info(f"Заданние в PLC отправленно корректно")
                return True
            else:
                self.logger.info(f"Заданние {task_robot} в PLC отправленно НЕ корректно. "
                                 f"Оно не соответствует {task_sql}")
                return False
        except Exception as error:
            self.logger.error(f"{self.robot_name}: Не удалось отправить данные в PLC: post_task не сработал"
                              f"{str(error)} {traceback.format_exc()}")
        return False

    def post_start(self):
        # Подготавливаем данные
        data_start = bytearray(2)
        # Пороверяем, записан ли стартEx
        if self.task_status != 2:
            self.logger.warning(f"{self.robot_name}: {self.task['task']} - Запуск робота")
            set_bool(data_start, 0, 0, True)
            set_int(data_start, 2, 1)
            self.plc_client.db_write(self.db_task, self.start_byte_task + self.start_byte_task_status, data_start)
            return False
        elif self.task_status == 2:
            self.logger.info(f"{self.robot_name}: {self.task['task']} - Робот начал Перекладку")
            return True

    def wait_finish(self):
        if self.quantity != self.old_current_sheet_data and self.quantity > 0:
            self.old_current_sheet_data = self.quantity
            self.sql.set_quantity(self.quantity, self.task['task'])
            self.logger.warning(
                f"{self.robot_name}: {self.task['task']} - Робот переложил {self.quantity}")

    def data_read(self):
        try:
            # Read DB
            # T O D O Один запрос покрывающий весь диапазон - далее парсинг
            data_finish = self.plc_client.db_read(self.db_task,
                                                  self.start_byte_datatask_whole + self.start_byte_datatask_status,
                                                  self.length_datatask_status)
            data_moving = self.plc_client.db_read(self.db_task,
                                                  self.start_byte_datatask_whole + self.start_byte_datatask_moving,
                                                  self.length_datatask_moving)
            data_sheet = self.plc_client.db_read(self.db_task,
                                                 self.start_byte_datatask_whole + self.start_byte_datatask_sheet,
                                                 self.length_datatask_sheet)
            data_id = self.plc_client.db_read(self.db_task,
                                              self.start_byte_datatask_whole + self.start_byte_datatask_id,
                                              self.length_datatask_id)
            data_control = self.plc_client.db_read(self.db_control, self.start_byte_control, self.length_DataControl)
            data_status = self.plc_client.db_read(self.db_status, self.start_byte_status, self.length_DataStatus)
            data_message = self.plc_client.db_read(self.db_messages, self.start_byte_message,
                                                   self.length_DataDataExtToServer)

            # Create variables
            thickness = []
            cover = []
            self.task_status = get_int(data_finish, 2)
            for i in range(get_int(data_moving, 4)):
                thickness_read = get_int(data_sheet, i * 2) % 256
                cover_read = (get_int(data_sheet, i * 2) - thickness_read) // 256
                thickness.append(thickness_read)
                cover.append(cover_read)

            # Preparation Data
            self.task_robot = {'zone_from': get_int(data_moving, 0),
                               'zone_to': get_int(data_moving, 2),
                               'required': get_int(data_moving, 4),
                               'cover': cover,
                               'thickness': thickness,
                               'robot_task_id': get_string(data_id, 0, 256),
                               'order_operation_id': get_string(data_id, 256, 256)}
            self.quantity = get_int(data_status, 6)
            self.printer.print_stick(data_control, data_message)

        except Exception as error:
            self.logger.error(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())}\n"
                              f"{self.robot_name}: Не удалось прочитать данные с PLC:\n"
                              f"{str(error)} {traceback.format_exc()}")

    def change_zone(self, zone):
        zone_changed = 0
        if self.task['robot_number'] == '1' and zone == 4 or self.task['robot_number'] == '2' and zone == 6:
            zone_changed = 1
        elif self.task['robot_number'] == '1' and zone == 1 or self.task['robot_number'] == '2' and zone == 3:
            zone_changed = 2
        elif zone == 2:
            zone_changed = 3
        elif zone == 5:
            zone_changed = 4
        return zone_changed


class Printer:
    def __init__(self, name, plc_client, start_byte_control):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format="[%(asctime)s: %(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s",
                            level=logging.WARNING, datefmt='%Y-%m-%d %H:%M:%S')
        self.moviesql = MovieSQL()
        self.connection = self.moviesql.get_printer(name)
        self.plc_client = plc_client
        self.timeframe = {'start': None, 'finish': None}
        self.uuid = 0
        self.start_byte_control = start_byte_control
        self.db_control = 601
        self.start_byte_printer = 24
        self.wait_list_finish = False
        self.name = name
        self.count_tries = 0
        self.command_stickon_ok = False

    def print_stick(self, data_control, data_message):
        # Data taken from input
        printstart = get_bool(data_control, self.start_byte_printer, 0)
        printdone = get_bool(data_control, self.start_byte_printer, 1)
        readyprintstick = get_bool(data_control, self.start_byte_printer, 2)
        readymakevideo = get_bool(data_control, self.start_byte_printer, 3)
        self.sheet_number = get_int(data_message, 2)

        # Принтер: Печать этикеток
        if self.wait_list_finish and not printstart and not self.command_stickon_ok:
            self.gripper_stickon()
            self.count_tries += 1
            if self.count_tries >= 5:
                readymakevideo = True
        else:
            self.command_stickon_ok = True

        if readyprintstick and not self.wait_list_finish:
            self.timeframe['start'] = datetime.now()  # начало перекладки
            self.uuid = uuid.uuid4()
            self.link = self.create_url(self.uuid, "V1DE0BOT")
            self.printer_print(self.link, self.sheet_number)
            self.gripper_stickon()

        if self.wait_list_finish and readymakevideo:
            self.count_tries = 0
            self.wait_list_finish = False
            self.command_stickon_ok = False
            self.timeframe['finish'] = datetime.now()
            self.moviesql.set_videodata(self.uuid, self.link, self.timeframe, self.connection["camera"],
                                        self.sheet_number, printdone, self.name)

    def printer_print(self, link, sheetnumber):
        self.logger.info(f"Печать этикетыки с линком {link} и номером {sheetnumber}")
        try:
            mysocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            run = "^XA"
            print_date = f"^FO5,20^ASN,20,18^FD{datetime.now().strftime('%Y.%m.%d')}^FS"
            print_time = f"^FO19,50^ASN,20,18^FD{datetime.now().strftime('%H:%M:%S')}^FS"
            sheetnumber = f'^FO50,100^ASN,120,120^FD{sheetnumber}^FS'
            qr_code = f'^FO200,20^BQN,2,4.4^FDHA,{link}^FS'
            end = "^XZ"
            data = run + print_date + print_time + sheetnumber + qr_code + end
            send_data = data.encode('utf-8')
            try:
                mysocket.connect(
                    (self.connection["printer"]["ip"], self.connection["printer"]["host"]))  # connecting to host
                mysocket.send(send_data)
                mysocket.close()
            except Exception as error:
                self.logger.error(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())}\n"
                                  f"Error with the connection:\n"
                                  f"{str(error)} {traceback.format_exc()}")
            self.logger.info(f"Этикетка отправлена на печать")
        except Exception as error:
            self.logger.error(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())}\n"
                              f"Не удалось отправить на печать:\n"
                              f"{str(error)} {traceback.format_exc()}")

    def gripper_stickon(self):
        data_write = bytearray(1)
        set_bool(data_write, 0, 0, True)
        set_bool(data_write, 0, 4, True)
        try:
            self.plc_client.db_write(self.db_control, self.start_byte_control + self.start_byte_printer, data_write)
            self.logger.info(f"Этикетка отправлена на наклейку")
            self.wait_list_finish = True  # ожидаем окончания перекладки листа
        except Exception as error:
            self.logger.error(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())}\n"
                              f"Не удалось наклеить этикетку:\n"
                              f"{str(error)} {traceback.format_exc()}")

    def create_url(self, payload, bot_username):
        payload = str(payload)
        bytes_payload: bytes = urlsafe_b64encode(payload.encode())
        str_payload = bytes_payload.decode()
        str_payload.replace("=", "")
        if len(payload) > 64:
            raise ValueError('Encoded payload must be up to 64 characters long.')
        return f"https://t.me/{bot_username}?start={str_payload}"
