from setuptools import setup, find_packages

paste_factory = ['crystal_filter_handler = '
                 'crystal_filter_middleware.crystal_filter_handler:filter_factory']

setup(name='swift_crystal_filter_middleware',
      version='0.0.5',
      description='Crystal filter middleware for OpenStack Swift',
      author='The AST-IOStack Team: Josep Sampe, Raul Gracia',
      url='http://iostack.eu',
      packages=find_packages(),
      requires=['swift(>=1.4)','storlets(>=1.0)'],
      entry_points={'paste.filter_factory':paste_factory}
      )
