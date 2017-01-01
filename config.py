# edit the URI below to add your RDS password and your AWS URL

# format: (user):(password)@(db_identifier).amazonaws.com:3306/(db_name)

#Uncomment the line below if you want to work with AWS RDS
# SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://flask:test1234@flasktest.c3myzyuhtacu.us-west-1.rds.amazonaws.com:3306/flaskdb'

# Uncomment the line below if you want to work with a local DB
#SQLALCHEMY_DATABASE_URI = 'sqlite:///test.db'

# SQLALCHEMY_POOL_RECYCLE = 3600

WTF_CSRF_ENABLED = True
SECRET_KEY = 'dsaf0897sfdg45sfdgfdsaqzdf98sdf0a'