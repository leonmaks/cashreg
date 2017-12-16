"""Test os.system"""
import os

exeName = '"D:\\utils\\python\\python36\\lib\\site-packages\\win32\\PythonService.exe"'
serviceName = 'CashregService'
svcArgs = ''

os.system("%s -debug %s %s" % (exeName, serviceName, svcArgs))
