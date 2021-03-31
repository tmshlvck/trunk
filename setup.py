#!/usr/bin/env python

from setuptools import setup

setup(name='trunk',
    version='1.0',
    description='Trunk - TCP proxy and telemetry over MQTT',
    install_requires = [
        'paho-mqtt',
        'asyncio-mqtt',
        'click',
        ],
    scripts = [
        'trunk.py',
        ],
   )

