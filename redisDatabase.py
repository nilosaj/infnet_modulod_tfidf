import redis


def connectRedis():
    r = redis.Redis(
        host='192.168.0.12',
        port=32770,
        password='',
        decode_responses=True)
    return r