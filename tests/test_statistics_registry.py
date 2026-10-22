import unittest

from statistics_plugins.registry import ALL_STATISTICS, VERIFICATION_STATISTICS


class StatisticsRegistryTests(unittest.TestCase):
    def test_every_definition_has_truthful_storage_and_capabilities(self):
        names = set()
        for entry in ALL_STATISTICS:
            spec = entry.spec
            self.assertNotIn(spec.name, names)
            names.add(spec.name)
            self.assertIn(spec.scope, {"region", "region_pooled", "ar_episode", "display"})
            self.assertTrue(spec.storage.kind)
            self.assertTrue(spec.storage.export_dtype)

    def test_only_verification_plugins_advertise_accumulator_lifecycle(self):
        verification_ids = {id(entry) for entry in VERIFICATION_STATISTICS}
        for entry in ALL_STATISTICS:
            has_lifecycle = all(hasattr(entry, name) for name in (
                "init_accumulator", "update", "finalize",
            ))
            self.assertEqual(has_lifecycle, id(entry) in verification_ids)


if __name__ == "__main__":
    unittest.main()
