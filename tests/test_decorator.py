__author__ = 'Bohdan Mushkevych'

import unittest
from synergy.system.decorator import base_model_derived
from synergy.db.model.base_model import BaseModel


# model description fields
FIELD_ENTRY_ID = 'entry_id'
FIELD_TITLE = 'title'
FIELD_VERSION = 'version'
FIELD_PLATFORM = 'platform'
FIELD_MODEL_A = 'model_a'
FIELD_MODEL_B = 'model_b'


class ModelA(BaseModel):
    def __init__(self, document=None):
        super(ModelA, self).__init__(document)

    @property
    def entry_id(self):
        return self.data.get(FIELD_ENTRY_ID)

    @entry_id.setter
    def entry_id(self, value):
        self.data[FIELD_ENTRY_ID] = value

    @property
    def title(self):
        return self.data.get(FIELD_TITLE)

    @title.setter
    def title(self, value):
        self.data[FIELD_TITLE] = value

    @classmethod
    def create_instance(cls):
        instance = ModelA()
        instance.entry_id = 'Entry A'
        instance.title = 'Title A'
        return instance


class ModelB(BaseModel):
    def __init__(self, document=None):
        super(ModelB, self).__init__(document)

    @property
    def platform(self):
        return self.data.get(FIELD_PLATFORM)

    @platform.setter
    def platform(self, value):
        self.data[FIELD_PLATFORM] = value

    @property
    def version(self):
        return self.data.get(FIELD_VERSION)

    @version.setter
    def version(self, value):
        self.data[FIELD_VERSION] = value

    @classmethod
    def create_instance(cls):
        instance = ModelB()
        instance.platform = 'Platform B'
        instance.version = 'Version B'
        return instance


class Encapsulator(BaseModel):
    def __init__(self, document=None):
        super(Encapsulator, self).__init__(document)

    @property
    @base_model_derived(ModelA)
    def instance_a(self):
        return self.data.get(FIELD_MODEL_A)

    @instance_a.setter
    @base_model_derived(ModelA)
    def instance_a(self, value):
        self.data[FIELD_MODEL_A] = value

    @property
    @base_model_derived(ModelB)
    def instance_b(self):
        return self.data.get(FIELD_MODEL_B)

    @instance_b.setter
    @base_model_derived(ModelB)
    def instance_b(self, value):
        self.data[FIELD_MODEL_B] = value


class TestDecorator(unittest.TestCase):
    def test_setter(self):
        encapsulator = Encapsulator()
        model_a = ModelA.create_instance()
        model_b = ModelB.create_instance()

        encapsulator.instance_b = model_b
        encapsulator.instance_a = model_a

        self.assertIsInstance(encapsulator.instance_b, ModelB)
        self.assertIsInstance(encapsulator.instance_a, ModelA)
        self.assertIsInstance(encapsulator.data[FIELD_MODEL_B], dict)
        self.assertIsInstance(encapsulator.data[FIELD_MODEL_A], dict)

    def test_getter(self):
        encapsulator = Encapsulator()
        model_a = ModelA.create_instance()
        model_b = ModelB.create_instance()

        encapsulator.instance_b = model_b.data
        encapsulator.instance_a = model_a.data

        self.assertIsInstance(encapsulator.instance_b, ModelB)
        self.assertIsInstance(encapsulator.instance_a, ModelA)
        self.assertIsInstance(encapsulator.data[FIELD_MODEL_B], dict)
        self.assertIsInstance(encapsulator.data[FIELD_MODEL_A], dict)

    def test_none(self):
        encapsulator = Encapsulator()
        model_b = ModelB.create_instance()
        encapsulator.instance_b = model_b.data

        self.assertIsInstance(encapsulator.instance_b, ModelB)
        self.assertIsNone(encapsulator.instance_a)
        self.assertIsInstance(encapsulator.data[FIELD_MODEL_B], dict)
        self.assertTrue(FIELD_MODEL_A not in encapsulator.data)


if __name__ == '__main__':
    unittest.main()
