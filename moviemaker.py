import os
import re
import subprocess
import msgpack
import requests
import logging
import traceback
from requests.auth import HTTPDigestAuth
from datetime import datetime, timedelta
from config import login, password, maxduration, cameras
from time import sleep

class MovieMaker:

    def __init__(self, numbercamera, filepath, robot_name):

        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format="[%(asctime)s: %(filename)s:%(lineno)s  - %(funcName)20s() ] %(message)s",
                            level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
        # self.filepath = "/home/kipia/Video/"
        self.numbercamera = numbercamera
        self.filepath = filepath
        self.robot_name = robot_name
        self.link = None
        self.logger.info(f"Экземпляр MovieMaker создан, файлы расположены {self.filepath}{robot_name}*")

    def getVideo(self, start_timeframe, finish_timeframe, timeframe_delta, video_link):
        self.logger.info(f"Отложенный запуск 15сек")
        # sleep(15)
        self.logger.info(f"Выкачиваем видео, после 15сек ожидагия")

        # Формируем временные метки
        self.link = f"{self.robot_name}_{video_link}.avi"
        timeframe = self.timeframe_creation(start_timeframe, finish_timeframe, timeframe_delta)
        if not timeframe:
            return False
        self.logger.info(f"Время начала записи: {timeframe['start']}, время конца записи: {timeframe['finish']}. "
                         f"Камера {self.numbercamera}. Длинна {timeframe['duration']} сек")

        # Проверяем существует ли локальный каталог, если не существует, то создаем.
        try:
            if not os.path.exists(self.filepath):
                os.makedirs(self.filepath)
        except Exception as error:
            self.logger.error(f"Ошибка {str(error)} {traceback.format_exc()}")
            return False

        # имя файла видео без расширения. Например video1.mp4 -> video1
        self.logger.info(f"{self.link}")
        file_name_w_ext = re.match(r"(.*)\..*$", self.link).group(1)

        self.logger.info(f"Качаем видео с сервера...")
        result_of_get_video_from_server = self.getVideFromServer(timeframe, self.numbercamera,
                                                                 self.filepath + file_name_w_ext + '.h264')

        if result_of_get_video_from_server > 0:
            self.logger.info(f"Конвертируем видео...")
            self.convert(self.filepath + file_name_w_ext + '.h264', self.filepath + self.link)
            self.logger.info(f"Видео готово...")
            return 1
        else:
            self.logger.info(f"В архиве нет видео с такими параметрами...")
            return 0

    def convert(self, input_file, output_file):
        self.logger.debug(f"Выполняем конвертацию исходного файла {input_file} в {output_file}")
        try:
            command = f'ffmpeg -i "{input_file}" "{output_file}"'
            try:
                os.remove(output_file)  # удаляем видеофайл, если он уже существует
            except OSError:
                pass
            subprocess.call([command], shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                            timeout=300)
            self.logger.info(f"Конвертация прошла успешно")
            os.remove(input_file)
            self.logger.info(f"Удалили h264")
        except subprocess.CalledProcessError as error:
            self.logger.error("Ошибка:\ncmd:{}\noutput:{}".format(error.cmd, error.output))
            self.logger.error(f"Ошибка {str(error)} {traceback.format_exc()}")
            raise
        self.logger.debug(f"Конвертация выполнена")

    def getVideFromServer(self, timeframe, channel, result_file_name):
        # Собираем запрос для получения списка id всех кадров с нужной камеры за указанный период времени:
        global result_file
        request_data = msgpack.packb({"method": "archive.get_frames_list",
                                      "params":
                                          {"channel": cameras[channel].get("cameraId"),
                                           # id этой камеры на сервере видеонаблюдения
                                           "stream": "video",
                                           # у  всех камер из списка есть толлько один поток с видео - "video"
                                           "start_time": timeframe["start"],  # дата и время начала записи в формате
                                           "end_time": timeframe["finish"]  # дата и время начала записи
                                           }
                                      })

        # Получем список id всех кадров с нужной камеры за указанный период времени:
        try:
            frame_list = requests.post(cameras[channel].get("cameraURL"), auth=HTTPDigestAuth(login, password),
                                       data=request_data, headers={'Content-Type': 'application/x-msgpack'})
            frames = msgpack.unpackb(frame_list.content)  # Распаковываем ответ сервера с помощью msgpack
        except Exception as error:
            self.logger.error(f"Ошибка запроса. {error}")
            return False

        if frames.__contains__("result"):
            # создаем массив gop(групп кадров) для скачивания,
            # отбрасываем в начале списка все кадры до первого опорного - они не нужны:
            frames_id = []  # массив gop(групп кадров)
            key_frame = 0
            for frame in frames['result']['frames_list']:
                if (key_frame == 0) and (frame['gop_index'] != 0):  # не ключевой кадр в начале видео, отбрасываем
                    pass
                else:
                    if frame['gop_index'] == 0:
                        key_frame += 1
                        frames_id.append([])
                    frames_id[key_frame - 1].append(frame['id'])
            if len(frames_id) > 0:
                result_file = open(result_file_name, 'wb')  # создаем файл, который станет видео
            for frame_keys in frames_id:
                for frame in frame_keys:
                    data = msgpack.packb({"method": "archive.get_frame",
                                          "params": {"channel": cameras[channel].get("cameraId"), "stream": "video",
                                                     "id": frame}})  # собираем запрос для получения кадра из архива по id
                    getFrameRequest = requests.post(cameras[channel].get("cameraURL"),
                                                    auth=HTTPDigestAuth(login, password),
                                                    data=data, headers={'Content-Type': 'application/x-msgpack'})
                    payload = msgpack.unpackb(getFrameRequest.content, raw=True)

                    # пишем в файл данные из поля raw_bytes ответа сервера
                    result_file.write(payload[b'result'][b'frame'][b'raw_bytes'])
            if len(frames_id) > 0:
                result_file.close()
            self.logger.info(f"Если кейфреймы найдены, то они были скачаны")
            return len(frames_id)
        else:
            return 0

    def timeframe_creation(self, start_timeframe, finish_timeframe, timeframe_delta):
        if isinstance(start_timeframe, datetime) and isinstance(start_timeframe, datetime):
            start = start_timeframe - timedelta(seconds=timeframe_delta)
            finish = finish_timeframe + timedelta(seconds=timeframe_delta)
            duration = int((finish - start).total_seconds())
            self.logger.info(f"Итоговая длина видео {duration} секунд")

            if duration <= maxduration:
                start = [start.year, start.month, start.day, start.hour, start.minute, start.second]
                finish = [finish.year, finish.month, finish.day, finish.hour, finish.minute, finish.second]
                timeframe = {"start": start, "finish": finish, "duration": duration}
                return timeframe
            self.logger.error(f"Длина видео: {duration} сек. Это больше разрешенного {maxduration} сек")
            return False

        self.logger.error(f"Не корректный формат времени dt_start: {start_timeframe} {type(start_timeframe)} или "
                          f"dt_finish: {finish_timeframe} {type(finish_timeframe)}. {traceback.format_exc()}")
        return False

    def pathname_creation(self):

        pass
