import redis


def connectRedis():
    r = redis.Redis(
        host='localhost',
        port=32769,
        password='')
    return r