==========================
Biometrics Tracker Project
==========================
Installation
------------
You will need a current working Python interpreter on your system, preferably version 3.10 or newer.  To download an
interpreter install package, go to <https://www.python.org> .  If you have an existing Python installation, and you
wish to download the Biometrics Tracker application for the Python Package repository (PyPI), you should upgrade
the installation tool, pip to the newest version by entering the command :code:`pip upgrade pip` at a command prompt.
The Biometrics Tracker can be downloaded from PyPI entering the command :code:`pip install biometrics-tracker`
at a command prompt.  Once the app is installed, it can be started by entered the command :code:`biotrack`

The application is also available from my GitHub repository at <https://github.com/stroudcuster/biometrics-tracker/>.
API documentation is available at <https://stroudcuster.github.io/biometrics-tracker/index.html>

This application provides allows you to record and track the following biometric data for a
number of people:

+  Blood Pressure
+  Pulse
+  Blood Glucose
+  Weight
+  Temperature

The application allows you to specify which metrics will be tracked for each person,
and what unit of measure will be used in recording each measurement. This makes the entry
of data more efficient, as no prompts or entry fields are displayed for metrics that are
not tracked for the person whose data is being entered, viewed or edited.

Two methods of data entry are provided: a display suited for the entry of one or more
metrics for a single date and time, and a history edit that allows you to quickly locate
and change data that was previously entered or imported.

In addition to the numerical reading, you may enter a note that provides context for the reading,
for instance whether the reading was taken before or after a meal or exercise.

The application provides several ways of viewing historical data for a specified period of time:
+  It can be viewed and edited on a GUI display
+  A report can be produced in text or PDF format
+  An export file can be created in several different formats

Help documentation is included with the application download.  It is available through the Help menu option on
the application GUI, and is displayed in a browser window.

I developed this application while recovering from heart surgery and dealing with a new diabetes diagnosis.
I was recording my readings in a Libre Office spreadsheet, but even the best spreadsheet app makes a less
than satisfactory database.

If you've been using a spreadsheet or some other application to track your metrics, the application can
import data from a comma separated values (CSV) file.  The import function allows you to specify the content
and column order of the file you are importing and reports any problems encountered while importing the data.

This application is written in Python.  It was developed using Python 3.10 and uses the following external libraries,
which are will be downloaded during the installation of the Biometrics Tracker application, if they are not
already on your system:

+ ttkbootstrap - GitHub repository <https://github.com/israel-dryer/ttkbootstrap> Documentation <https://ttkbootstrap.readthedocs.io/en/latest/>
+ fpdf2 - GitHub repository <https://github.com/reingart/pyfpdf> Documentation <https://pyfpdf.readthedocs.io/en/latest/>
+ schedule - GitHub repository <https://github.com/dbader/schedule> Documentation <https://schedule.readthedocs.io/en/stable/>

I used the Pycharm Community Edition IDE to develop the application.  Sphinx was used to build the API documentation.
User documents were created directly in HTML with the Bluefish editor, as Sphinx gave no small amount of trouble in
creating these documents.

The database, scheduling and reporting functions are run as separate threads to avoid tying up the GUI's event processing
loop. I would like to have more separation between the application logic and the GUI, but I was more concerned that the
application's functionality be reasonably complete.

The Biometrics Tracker application and related documentation are  published under the MIT open source license and the
code is available for anyone to view.  Your data stays on your computer and there are no ads or in-app purchases.





