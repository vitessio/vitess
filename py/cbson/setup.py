from distutils.core import setup, Extension

cbson = Extension('cbson',
                  sources = ['cbson.c'])

setup(name = 'cbson',
      version = '0.1',
      description = 'Fast BSON decoding via C',
      ext_modules = [cbson])
