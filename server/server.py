from aiohttp import web
from queue_client.client import RedisClient

redis_cli = RedisClient()


async def accept(request):
    data = await request.json()
    robot_name = data[0]['device']
    data = sorted(data, key=lambda val: val['i'])

    if redis_cli.add_tasks(robot_name, data):
        return web.json_response({'status': '201'})

    return web.json_response({'status': '500'})


app = web.Application()
app.add_routes([web.post('/', accept)])

