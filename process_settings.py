ENVIRONMENT = '%ENVIRONMENT%'


def register_processes():
    # For now just two level... we can have configs for all deployments
    # Need to have a better way for switching these
    try:
        overrides = __import__('process_settings_' + ENVIRONMENT)
    except:
        overrides = __import__('process_settings_dev')
    overrides.register_processes()
