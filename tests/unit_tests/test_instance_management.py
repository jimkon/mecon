import unittest

from mecon.utils.instance_management import Singleton, Multiton, InstanceDoesNotExistError, InstanceAlreadyExistError


class TestSingleton(unittest.TestCase):
    def test_singleton_instance(self):
        # Ensure that multiple instances are the same
        singleton1 = Singleton.get_instance()
        singleton2 = Singleton.get_instance()
        self.assertIs(singleton1, singleton2)

    def test_set_instance(self):
        # Create a new instance and set it using set_instance
        new_singleton = Singleton()
        Singleton.set_instance(new_singleton)
        singleton = Singleton.get_instance()
        self.assertIs(new_singleton, singleton)

    def test_reset_instance(self):
        # Create a new instance, set it, and then reset it
        new_singleton = Singleton()
        Singleton.set_instance(new_singleton)
        Singleton.reset_instance()
        self.assertIsNone(Singleton._instance)


class TestMultiton(unittest.TestCase):
    def setUp(self):
        class DummyMultiton(Multiton):
            pass

        self.DummyMultiton = DummyMultiton

    def test_creation(self):
        instance_name = "instance1"
        instance = self.DummyMultiton(instance_name)
        self.assertEqual(instance._instance_name, instance_name)

    def test_duplicate_creation(self):
        instance_name = "instance2"
        self.DummyMultiton(instance_name)
        with self.assertRaises(InstanceAlreadyExistError):
            self.DummyMultiton(instance_name)

    def test_from_key_existing(self):
        instance_name = "instance3"
        instance = self.DummyMultiton(instance_name)
        retrieved_instance = self.DummyMultiton.from_key(instance_name)
        self.assertEqual(instance, retrieved_instance)

    def test_from_key_nonexistent(self):
        with self.assertRaises(InstanceDoesNotExistError):
            self.DummyMultiton.from_key("nonexistent_instance")

    def test_all_instances(self):
        instance1 = self.DummyMultiton("instance4")
        instance2 = self.DummyMultiton("instance5")
        all_instances = self.DummyMultiton.all_instances()
        self.assertIn(instance1, all_instances)
        self.assertIn(instance2, all_instances)

    def test_different_multiton_subclasses(self):
        class Subclass1(Multiton):
            pass

        class Subclass2(Multiton):
            pass

        instance1_subclass1 = Subclass1("instance1_subclass1")
        instance2_subclass2 = Subclass2("instance2_subclass2")

        retrieved_instance1_subclass1 = Subclass1.from_key("instance1_subclass1")
        retrieved_instance2_subclass2 = Subclass2.from_key("instance2_subclass2")

        self.assertEqual(retrieved_instance1_subclass1.instance_name, "instance1_subclass1")
        self.assertEqual(retrieved_instance2_subclass2.instance_name, "instance2_subclass2")

        # Attempt to access instances from the other subclass
        with self.assertRaises(InstanceDoesNotExistError):
            Subclass1.from_key("instance2_subclass2")

        with self.assertRaises(InstanceDoesNotExistError):
            Subclass2.from_key("instance1_subclass1")

        # already existing instances
        Subclass1("instance2_subclass2")
        with self.assertRaises(InstanceAlreadyExistError):
            Subclass1("instance1_subclass1")

        Subclass2("instance1_subclass1")
        with self.assertRaises(InstanceAlreadyExistError):
            Subclass2("instance2_subclass2")

    def test_deep_multiton_inheritance(self):
        class Subclass1(Multiton):
            pass

        class Subclass11(Subclass1):
            pass

        class Subclass12(Subclass1):
            pass

        Subclass1("instance1_subclass1")
        Subclass11("instance2_subclass11")
        Subclass11("instance3_subclass12")

        self.assertEqual(Subclass1.from_key("instance1_subclass1").instance_name, "instance1_subclass1")
        self.assertEqual(Subclass11.from_key("instance2_subclass11").instance_name, "instance2_subclass11")
        self.assertEqual(Subclass12.from_key("instance3_subclass12").instance_name, "instance3_subclass12")

        self.assertEqual(Subclass1.from_key("instance1_subclass1"), Subclass11.from_key("instance1_subclass1"))
        self.assertEqual(Subclass1.from_key("instance1_subclass1"), Subclass12.from_key("instance1_subclass1"))

        self.assertEqual(Subclass1.from_key("instance2_subclass11"), Subclass11.from_key("instance2_subclass11"))
        self.assertEqual(Subclass1.from_key("instance2_subclass11"), Subclass12.from_key("instance2_subclass11"))

        self.assertEqual(Subclass1.from_key("instance3_subclass12"), Subclass11.from_key("instance3_subclass12"))
        self.assertEqual(Subclass1.from_key("instance3_subclass12"), Subclass12.from_key("instance3_subclass12"))


if __name__ == '__main__':
    unittest.main()
