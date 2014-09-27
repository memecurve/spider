from api.services.db import HbaseInternals

c = HbaseInternals()
c.write('wordcount', 'foobar', {'bin:12345': 1}) 
c.inc('wordcount', 'foobar', 'bin:12345', 10)
