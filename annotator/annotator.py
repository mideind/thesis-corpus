#!/usr/bin/env python

# Run me with (requires proper venv)
#   FLASK_APP=annotator.py flask run
# or for development
#   FLASK_ENV=development FLASK_APP=annotator.py flask run

import os
from pathlib import Path

from flask import Flask, Response, render_template, request
app = Flask(__name__)


def get_unannotated_files():
    # TODO compare with list of already annotated files?
    files = []
    for (dirpath, dirnames, filenames) in os.walk("static/textfiles"):
        files.extend([str(Path(dirpath)/fn) for fn in filenames if fn.endswith(".txt")])
    print(files)
    return files

@app.route("/")
def hello_world():
    return render_template("annotator.html", unannotated=get_unannotated_files())

@app.route("/save", methods=['POST'])
def save():
    data = request.data.decode('utf-8').split('\n', 1)
    filename = data[0]
    contents = data[1]

    print("fn: ", filename)
    print(contents)

    try:
        # Yes, we're trusting the client
        with open(filename+'.annotated', 'w') as f:
            f.write(contents)
    except Exception as e:
        return str(e)
    
    return ""
