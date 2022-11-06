from setuptools import setup

setup(
    name='biometrics_tracker',
    description='This application records and reports biometric measurements (e.g. weight, blood pressure, pulse)',
    version='1.0.0',
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
