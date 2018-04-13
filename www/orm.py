#_*_coding:utf-8_*_
import asyncio, logging, aiomysql

def log(sql, args=()):
    logging.info('SQL: %s'%sql)

#创建数据库连接池
@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('Create database connection pool....')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

#执行select语句
@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned:%s'%len(rs))
        return rs

#insert update delete在同一语句中
@asyncio.coroutine
def execute(sql, args):
    log(sql)
    global __pool
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected
#下面开始定义ORM
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        #排除model类本身
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        #获取table的名称
        tableName = attrs.get('__table__', None) or name
        logging.info('found model:%s (table: %s)'%(name, tableName))
        #获取所有的Field和主键名
        mappings = dict()#字典 key-value
        fields = []#存放除主键以外的key名称
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s==>%s'%(k, v))
                mappings[k] = v
                if v.primary_key:
                    #找到主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field:%s'%k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings:
            attrs.pop(k) #从属性列表中删除Field字段
        escaped_fields = list(map(lambda f:'%s'%f, fields))#不知道是做什么的
        attrs['__mappings__'] = mappings #保存属性和列的映射
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields
        #构造默认的select、insert、update和delete语句 #也不知道有没有用
        attrs['__select__'] = 'select %s, %s from %s'%(primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into %s (%s, %s) VALUES (%s)'%(tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update %s set %s where %s=?'%(tableName, ','.join(map(lambda f:'%s=?'%(mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from %s where %s=?'%(tableName, primaryKey)


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute %s"%item)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key): #后续进一步研究
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s:%s'%(key, str(value)))
                setattr(self, key, value)
        return value

#表明各种字段的类
class Field(object):
    def __init__(self, name, colume_type, primary_key, default):
        self.name = name
        self.colume_type = colume_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>'%(self.__class__.__name__, self.colume_type, self.name)

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)






