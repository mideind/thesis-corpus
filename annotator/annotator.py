#!/usr/bin/env python

# Run me with (requires proper venv)
#   FLASK_APP=annotator.py flask run
# or for development
#   FLASK_ENV=development FLASK_APP=annotator.py flask run

import os

from flask import Flask
app = Flask(__name__)


my_global_variable = 0

@app.route('/')
def hello_world():
    global my_global_variable
    my_global_variable += 1

    return 'bye' + str(my_global_variable)

@app.route('/unannotated')
def get_unannotated():
    files = [f for f in os.listdir('static/unannotated')]
    return '\n'.join(files)

