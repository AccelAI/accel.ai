from app import app

@app.route('/')
@app.route('/index')
def index():
    return "Accel.AI"

@app.route('/workshops')
def workshops():
    return "workshops"

@app.route('/blog')
def blog():
    return "blog"

@app.route('/curriculum')
def curriculum():
    return "curriculum"

