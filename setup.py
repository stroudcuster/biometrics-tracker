from setuptools import setup

with open('biometrics_tracker/version.py') as f:
    exec(f.read())


setup(
    name='biometrics_tracker',
    description='This application records and reports biometric measurements (e.g. weight, blood pressure, pulse)',
    version=__version__,
    packages=['biometrics_tracker',
              'biometrics_tracker.main',
              'biometrics_tracker.gui',
              'biometrics_tracker.model',
              'biometrics_tracker.output',
              'biometrics_tracker.utilities',
              'biometrics_tracker.config',
              'biometrics_tracker.ipc',
              ]
)
