from plc.tags import TagsBuilder
from plc.plc_client import PLCClient


class PLC:
    def __init__(self, ip: str, db_tags):
        tb = TagsBuilder(PLCClient(ip), db_tags)
        self.__dict__.update(tb.build_tags())


if __name__ == '__main__':
    plc = PLC('192.168.29.1', 1000)
    # print(plc.strRobotTaskID.get())
    # print(plc.uiRequired.get())
    # print(plc.uiTaskStatus.get())
    # print(plc.uiZoneFrom.get())
    # print(plc.uiZoneTo.get())
    # print(plc.usiCover.get())
    # print(plc.usiThickness.get())

    {
        "task": "9d662741-6e09-4894-abb3-fccf9aa4b2e1",
        "pid": "0be81130-6328-11ec-90b8-0050569c2c21",
        "i": 4,
        "device": "k2vrn012",
        "cell": "001004925",
        "zone_out": 3,
        "zone_in": 2,
        "qty": 4,
        "dcreate": "2021-12-22T16:10:26",
        "cover": 1,
        "employee": "Жиляев Александр Борисович",
        "depth": 16,
        "bfinishforce": 0,
        "in_cell_numb": 0,
        "decore": "H2033 16мм ST10 2800*2070 мм Дуб Хантон темный Egger",
        "revolution": "false"
    }

    plc.strRobotTaskID.set("9d662741-6e09-4894-abb3-fccf9aa4b2e1")
    plc.uiRequired.set(4)
    plc.uiTaskStatus.set(1)
    plc.uiZoneFrom.set(2)
    plc.uiZoneTo.set(3)
    plc.usiCover.set([1, 1, 1, 1])
    plc.usiThickness.set([16, 16, 16, 16])

    print(plc.strRobotTaskID.get())
    print(plc.uiRequired.get())
    print(plc.uiTaskStatus.get())
    print(plc.uiZoneFrom.get())
    print(plc.uiZoneTo.get())
    print(plc.usiCover.get())
    print(plc.usiThickness.get())


    c = 1
