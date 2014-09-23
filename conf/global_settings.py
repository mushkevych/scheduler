__author__ = 'Bohdan Mushkevych'


settings = {
    'is_configured': False,
}

process_context = {
}


mq_queue_context = {
}


timetable_context = {
}


def configure(ext_settings, ext_process_context, ext_mq_context, ext_timetable_context):
    """ Called to manually configure the settings. """
    if settings['is_configured']:
        raise RuntimeError('Settings already configured.')
    settings.update(ext_settings)
    process_context.update(ext_process_context)
    mq_queue_context.update(ext_mq_context)
    timetable_context.update(ext_timetable_context)

    settings['is_configured'] = True
