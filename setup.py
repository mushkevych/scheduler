from distutils.core import setup

setup(name = 'synergy_scheduler',
      version = '1.8',
      description = 'Synergy Scheduler',
      author = 'Bohdan Mushkevych',
      author_email = 'mushkevych@gmail.com',
      url = 'https://github.com/mushkevych/scheduler',
      packages = ['synergy', 'synergy.db', 'synergy.db.dao', 'synergy.db.manager', 'synergy.db.model',
                  'synergy.mq', 'synergy.conf', 'synergy.mx', 'synergy.scheduler', 'synergy.supervisor',
                  'synergy.system', 'synergy.workers'],
      package_data = {'synergy.mx': ['static/images/*', 'static/fonts/*', 'static/js/*', 'static/css/*', 'templates/*'],
                      'synergy.mq': ['AUTHORS', 'LICENSE']},
      long_description = '''Synergy Scheduler works both as a simple cron-style scheduler and a more elaborate solution
      with multiple state machines governing processes and their jobs, as well as interdependencies between them.''',
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
          'Topic :: Office/Business :: Scheduling',
          'Topic :: Utilities',
          ],
      requires=['werkzeug', 'jinja2', 'amqp', 'pymongo', 'psutil', 'fabric', 'setproctitle', 'synergy_odm', 'mock',
                'xmlrunner', 'pylint', 'six']
)