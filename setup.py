from distutils.core import setup

setup(name = 'synergy_scheduler',
      version = '1.1',
      description = 'Synergy Scheduler',
      author = 'Bohdan Mushkevych',
      author_email = 'mushkevych@gmail.com',
      url = 'https://github.com/mushkevych/scheduler',
      packages = ['db', 'db.dao', 'db.manager', 'db.model', 'mq', 'mx', 'scheduler', 'supervisor', 'system', 'workers'],
      package_data = {'mx': ['static/*', 'templates/*'],
                      'mq': ['AUTHORS', 'LICENSE']},
      long_description = '''Really long text here.''',
      license = 'Modified BSD License',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Environment :: Web Environment',
          'Intended Audience :: End Users/Desktop',
          'Intended Audience :: Developers',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: BSD License',
          'Operating System :: POSIX',
          'Programming Language :: Python',
          'Programming Language :: JavaScript',
          'Topic :: Communications :: Email',
          'Topic :: Office/Business :: Scheduling',
          'Topic :: Utilities',
          ], requires=['werkzeug', 'werkzeug', 'werkzeug', 'jinja2']
)