#!/usr/bin/env python

# Run me with (requires proper venv)
#   FLASK_APP=annotator.py flask run
# or for development
#   FLASK_ENV=development FLASK_APP=annotator.py flask run

import os
from pathlib import Path

from flask import Flask, Response, render_template
app = Flask(__name__)


def get_unannotated_files():
    # TODO compare with list of already annotated files?
    files = []
    for (dirpath, dirnames, filenames) in os.walk("static/unannotated"):
        files.extend([str(Path(dirpath) / filename) for filename in filenames])
    return files

@app.route("/")
def hello_world():
    files = get_unannotated_files()

    return render_template("annotator.html", unannotated=files)


@app.route("/unannotated")
def get_unannotated():
    return Response("\n".join(get_unannotated_files()), mimetype="text/plain")
