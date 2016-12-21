import os
import unittest

from aws_etl_tools import config
from aws_etl_tools.guard import requires_s3_base_path
from aws_etl_tools.exceptions import NoS3BasePathError
from tests import test_helper


@requires_s3_base_path
def function_that_uses_s3_for_something(basic_arg):
    '''returns whatever you give it'''
    return basic_arg


class Arbitrary:
    '''a class that requires config.S3_BASE_PATH for its methods'''
    @classmethod
    @requires_s3_base_path
    def factory(cls):
        '''returns an instance of this class'''
        return cls()

    @property
    @requires_s3_base_path
    def true_property(self):
        '''is always true'''
        return True

    @requires_s3_base_path
    def instance_method(self, basic_arg):
        '''returns whatever you give it'''
        return basic_arg


class BaseTestingRequiresS3BasePath(unittest.TestCase):

    def setUp(self):
        self.initial_s3_base_path = config.S3_BASE_PATH

    def tearDown(self):
        config.S3_BASE_PATH = self.initial_s3_base_path

    def set_s3_base_path(self, new_value):
        config.S3_BASE_PATH = new_value


class TestClassMethodsThatRequireS3BasePath(BaseTestingRequiresS3BasePath):

    def test_factory_raises_exception_when_s3_base_path_is_not_set(self):
        self.set_s3_base_path(None)

        with self.assertRaises(NoS3BasePathError):
            Arbitrary.factory()


    def test_factory_works_as_expected_when_s3_base_path_is_set(self):
        self.set_s3_base_path('any-string-will-do')

        actual_return_value = Arbitrary.factory()

        self.assertIsInstance(actual_return_value, Arbitrary)


    def test_property_raises_exception_when_s3_base_path_is_not_set(self):
        self.set_s3_base_path(None)

        arbitrary_instance = Arbitrary()

        with self.assertRaises(NoS3BasePathError):
            arbitrary_instance.true_property


    def test_property_works_as_expected_when_s3_base_path_is_set(self):
        self.set_s3_base_path('any-string-will-do')

        arbitrary_instance = Arbitrary()

        self.assertEqual(arbitrary_instance.true_property, True)


    def test_instance_method_raises_exception_when_s3_base_path_is_not_set(self):
        self.set_s3_base_path(None)
        testable_input = 'foobar'

        with self.assertRaises(NoS3BasePathError):
            Arbitrary().instance_method(testable_input)


    def test_instance_method_works_as_expected_when_s3_base_path_is_set(self):
        self.set_s3_base_path('any-string-will-do')
        testable_input = 'foobar'

        return_value = Arbitrary().instance_method(testable_input)

        self.assertEqual(return_value, testable_input)


class TestTopLevelFunctionThatRequiresS3BasePath(BaseTestingRequiresS3BasePath):

    def test_raises_exception_when_s3_base_path_is_not_set(self):
        self.set_s3_base_path(None)
        testable_input = 'foobar'

        with self.assertRaises(NoS3BasePathError):
            function_that_uses_s3_for_something(testable_input)


    def test_function_works_as_expected_when_s3_base_path_is_set(self):
        self.set_s3_base_path('any-string-will-do')
        testable_input = 'foobar'

        return_value = function_that_uses_s3_for_something(testable_input)

        self.assertEqual(return_value, testable_input)
