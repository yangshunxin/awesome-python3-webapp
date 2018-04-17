#_*_coding:utf-8_*_
import orm
from models import User
import asyncio

@asyncio.coroutine
def test(loop):
    print('test() begin ')
    yield from orm.create_pool(user='root', password='password', db='awesome', loop=loop)
    u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank')
    yield from u.save()

if __name__=='__main__':#后面继续测试 TypeError: 'NoneType' object is not callable
    print('begin')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.close()
    print('end')
