"""
This module was adapted from Django's settings and configuration:
- https://raw.githubusercontent.com/django/django/master/django/conf/__init__.py
- https://raw.githubusercontent.com/django/django/master/django/utils/functional.py

Django is licensed under BSD. License is available at:
- https://github.com/django/django/blob/master/LICENSE
"""
import copy
import importlib
import operator
import os

from synergy.conf import global_context, global_settings

ENVIRONMENT_SETTINGS_VARIABLE = 'SYNERGY_SETTINGS_MODULE'
ENVIRONMENT_CONTEXT_VARIABLE = 'SYNERGY_CONTEXT_MODULE'


empty = object()


class ImproperlyConfigured(Exception):
    """Django is somehow improperly configured"""
    pass


def new_method_proxy(func):
    def inner(self, *args):
        if self._wrapped is empty:
            self._setup()
        return func(self._wrapped, *args)
    return inner


class LazyObject(object):
    """
    A wrapper for another class that can be used to delay instantiation of the
    wrapped class.

    By subclassing, you have the opportunity to intercept and alter the
    instantiation. If you don't need to do that, use SimpleLazyObject.
    """

    # Avoid infinite recursion when tracing __init__ (#19456).
    _wrapped = None

    def __init__(self):
        self._wrapped = empty

    __getattr__ = new_method_proxy(getattr)

    def __setattr__(self, name, value):
        if name == '_wrapped':
            # Assign to __dict__ to avoid infinite __setattr__ loops.
            self.__dict__['_wrapped'] = value
        else:
            if self._wrapped is empty:
                self._setup()
            setattr(self._wrapped, name, value)

    def __delattr__(self, name):
        if name == '_wrapped':
            raise TypeError("can't delete _wrapped.")
        if self._wrapped is empty:
            self._setup()
        delattr(self._wrapped, name)

    def _setup(self):
        """
        Must be implemented by subclasses to initialize the wrapped object.
        """
        raise NotImplementedError('subclasses of LazyObject must provide a _setup() method')

    def __deepcopy__(self, memo):
        if self._wrapped is empty:
            # We have to use type(self), not self.__class__, because the
            # latter is proxied.
            result = type(self)()
            memo[id(self)] = result
            return result
        return copy.deepcopy(self._wrapped, memo)

    __bytes__ = new_method_proxy(bytes)
    __str__ = new_method_proxy(str)
    __bool__ = new_method_proxy(bool)

    # Introspection support
    __dir__ = new_method_proxy(dir)

    # Need to pretend to be the wrapped class, for the sake of objects that
    # care about this (especially in equality tests)
    __class__ = property(new_method_proxy(operator.attrgetter('__class__')))
    __eq__ = new_method_proxy(operator.eq)
    __ne__ = new_method_proxy(operator.ne)
    __hash__ = new_method_proxy(hash)

    # List/Tuple/Dictionary methods support
    __getitem__ = new_method_proxy(operator.getitem)
    __setitem__ = new_method_proxy(operator.setitem)
    __delitem__ = new_method_proxy(operator.delitem)
    __iter__ = new_method_proxy(iter)
    __len__ = new_method_proxy(len)
    __contains__ = new_method_proxy(operator.contains)

    def configure(self, default_settings=global_settings, **options):
        """
        Called to manually configure the settings. The 'default_settings'
        parameter sets where to retrieve any unspecified values from (its
        argument must support attribute access (__getattr__)).
        """
        if self._wrapped is not empty:
            raise RuntimeError('Settings already configured.')
        holder = UserSettingsHolder(default_settings)
        for name, value in options.items():
            setattr(holder, name, value)
        self._wrapped = holder

    @property
    def configured(self):
        """ Returns True if the settings have already been configured. """
        return self._wrapped is not empty


class LazySettings(LazyObject):
    """ A lazy proxy for Synergy Settings """
    def _setup(self):
        """
        Load the settings module pointed to by the environment variable. This
        is used the first time we need any settings at all, if the user has not
        previously configured the settings manually.
        """
        settings_module = os.environ.get(ENVIRONMENT_SETTINGS_VARIABLE, 'settings')
        if not settings_module:
            raise ImproperlyConfigured(
                'Requested settings module points to an empty variable. '
                'You must either define the environment variable {0} '
                'or call settings.configure() before accessing the settings.'
                .format(ENVIRONMENT_SETTINGS_VARIABLE))

        self._wrapped = Settings(settings_module, default_settings=global_settings)

    def __getattr__(self, name):
        if self._wrapped is empty:
            self._setup()
        return getattr(self._wrapped, name)


class LazyContext(LazyObject):
    """ A lazy proxy for Synergy Context """
    def _setup(self):
        """
        Load the context module pointed to by the environment variable. This
        is used the first time we need the context at all, if the user has not
        previously configured the context manually.
        """
        context_module = os.environ.get(ENVIRONMENT_CONTEXT_VARIABLE, 'context')
        if not context_module:
            raise ImproperlyConfigured(
                'Requested context points to an empty variable. '
                'You must either define the environment variable {0} '
                'or call context.configure() before accessing the context.'
                .format(ENVIRONMENT_CONTEXT_VARIABLE))

        self._wrapped = Settings(context_module, default_settings=global_context)

    def __getattr__(self, name):
        if self._wrapped is empty:
            self._setup()
        return getattr(self._wrapped, name)


class BaseSettings(object):
    """ Common logic for settings whether set by a module or by the user. """
    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)


class Settings(BaseSettings):
    def __init__(self, settings_module, default_settings):
        # update this dict from global_settings or global_context
        for setting in dir(default_settings):
            setattr(self, setting, getattr(default_settings, setting))

        # store the settings module in case someone later cares
        self.SETTINGS_MODULE = settings_module

        mod = importlib.import_module(self.SETTINGS_MODULE)

        self._explicit_settings = set()
        for setting in dir(mod):
            setting_value = getattr(mod, setting)
            if isinstance(setting_value, dict) and hasattr(self, setting):
                existing = getattr(self, setting)
                existing.update(setting_value)
                setattr(self, setting, existing)
            else:
                setattr(self, setting, setting_value)
            self._explicit_settings.add(setting)

    def is_overridden(self, setting):
        return setting in self._explicit_settings


class UserSettingsHolder(BaseSettings):
    """ Holder for user configured settings. """
    # SETTINGS_MODULE doesn't make much sense in the manually configured (standalone) case.
    SETTINGS_MODULE = None

    def __init__(self, default_settings):
        """
        Requests for configuration variables not in this class are satisfied
        from the module specified in default_settings (if possible).
        """
        self.__dict__['_deleted'] = set()
        self.default_settings = default_settings

    def __getattr__(self, name):
        if name in self._deleted:
            raise AttributeError
        return getattr(self.default_settings, name)

    def __setattr__(self, name, value):
        self._deleted.discard(name)
        super(UserSettingsHolder, self).__setattr__(name, value)

    def __delattr__(self, name):
        self._deleted.add(name)
        if hasattr(self, name):
            super(UserSettingsHolder, self).__delattr__(name)

    def __dir__(self):
        return list(self.__dict__) + dir(self.default_settings)

    def is_overridden(self, setting):
        deleted = (setting in self._deleted)
        set_locally = (setting in self.__dict__)
        set_on_default = getattr(self.default_settings, 'is_overridden', lambda s: False)(setting)
        return deleted or set_locally or set_on_default


settings = LazySettings()
context = LazyContext()
