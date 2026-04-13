import app as application
import sys

app_ = application.App()
app = app_.app

if sys.argv[1] == "debug":
    print("IS DEBUG")
    app.run()