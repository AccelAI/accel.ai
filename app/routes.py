from flask import *
from app import app



@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html', title='Home')





@app.route('/blog')
def blog():
    return render_template('blog.html', title='Blog')


@app.route('/culture')
def culture():
      return render_template('culture.html', title='Culture')

@app.route('/curriculum')
def curriculum():
    return "curriculum"

@app.route('/resources')
def resources():
  return render_template('resources.html', title='Resources')

@app.route('/signup')
def signup():
    return render_template('signup.html', title='Signup')


@app.route('/workshops')
def workshops():
    return render_template('workshops.html', title='Workshops')



