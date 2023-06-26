## 服务器启动
MONGO_HOST = "127.0.0.1"
MONGO_PORT = 27017
MONGO_DB = 'foreign_news'
MONGO_USR = 'readWrite'
MONGO_PWD = 'readWrite123456'
SERVER_HOST = '127.0.0.1'   ### 上传服务器

## db.createUser({user:"readWrite",pwd:"readWrite123456",roles:[{role:"readWrite",db:"foreign_news"}]})

# # 本地启动
# MONGO_HOST = "127.0.0.1"
# MONGO_PORT = 27017
# MONGO_DB = 'foreign_news'
# MONGO_USR = ''
# MONGO_PWD = ''
# SERVER_HOST = '27.150.182.135'   ### 上传服务器

# 上传文章
ARTICLEUPLOAD = 'http://{0}:8855/articleMaterials/add'.format(SERVER_HOST)

MONGO_URI = f'mongodb://{MONGO_USR}:{MONGO_PWD}@{MONGO_HOST}:27017/{MONGO_DB}'

