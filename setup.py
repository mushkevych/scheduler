from distutils.core import setup

setup(name='synergy_scheduler',
      version='2.3',
      description='Synergy Scheduler',
      author='Bohdan Mushkevych',
      author_email='mushkevych@gmail.com',
      url='https://github.com/mushkevych/scheduler',
      packages=['synergy', 'synergy.db', 'synergy.db.dao', 'synergy.db.manager', 'synergy.db.model',
                'synergy.mq', 'synergy.conf', 'synergy.mx', 'synergy.scheduler', 'synergy.supervisor',
                'synergy.system', 'synergy.workers'],
      package_data={'synergy.mx': ['static/images/*', 'static/fonts/*', 'static/js/*', 'static/css/*', 'templates/*'],
                    'synergy.mq': ['AUTHORS', 'LICENSE']},
      long_description='Synergy Scheduler breaks time into intervals, associates every interval with a job '
                       'and makes sure that each job is completed in the right order. '
                       'Tracks dependencies between jobs. Has rich Web UI. '
                       'Can also work as a cron-like job trigger.',
      license='BSD 3-Clause License',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Environment :: Web Environment',
          'Intended Audience :: End Users/Desktop',
          'Intended Audience :: Developers',
          'Intended Audience :: Information Technology',
          'Intended Audience :: System Administrators',
          'License :: OSI Approved :: BSD License',
          'Operating System :: POSIX',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: JavaScript',
          'Topic :: Office/Business :: Scheduling',
          'Topic :: Utilities',
      ],
      install_requires=['werkzeug', 'jinja2', 'amqp', 'pymongo', 'psutil', 'setproctitle',
                        'synergy_odm>=0.10', 'synergy_flow>=0.16'],
      extras_require={'testing': ['mock', 'xmlrunner', 'pylint'],
                      'bash_driver': ['fabric2'],
                      }
      ),
