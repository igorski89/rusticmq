"""Regression checks for Docker audit evidence validation; no Docker required."""
import tempfile
import unittest
from pathlib import Path

from docker import EXPECTED_TESTS, check_test_count


class AuditEvidenceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.log = Path(directory.name) / "suite.log"

    def check(self, suite, output):
        self.log.write_text(output)
        return check_test_count(self.log, suite)

    def test_cached_or_empty_rust_output_is_rejected(self):
        for output in ("", "#14 RUN cargo test --locked --all-targets\n#14 CACHED\n",
                       "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out"):
            with self.assertRaises(RuntimeError):
                self.check("rust", output)

    def test_rust_requires_the_full_unfiltered_successful_suite(self):
        expected = EXPECTED_TESTS["rust"]
        output = f"#14 2.15 test result: ok. {expected} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.1s\n"
        self.assertEqual(self.check("rust", output), expected)
        for incomplete in (output.replace(f"{expected} passed", f"{expected - 1} passed"),
                           output.replace("0 ignored", "1 ignored"),
                           output.replace("0 filtered out", "1 filtered out"),
                           output.replace("0 failed", "1 failed")):
            with self.assertRaises(RuntimeError):
                self.check("rust", incomplete)

    def test_upstream_evidence_requires_both_java_and_node_results(self):
        output = "[info] Tests: succeeded 150, failed 0, canceled 0, ignored 0, pending 0\nTests:       5 passed, 5 total\n"
        self.assertEqual(self.check("upstream_java", output), 150)
        self.assertEqual(self.check("upstream_node", output), 5)
        with self.assertRaises(RuntimeError):
            self.check("upstream_node", "[info] All tests passed.\n")
        with self.assertRaises(RuntimeError):
            self.check("upstream_java", output.replace("succeeded 150", "succeeded 149"))

    def test_process_suite_cannot_silently_run_fewer_tests(self):
        self.assertEqual(self.check("restart", "Ran 5 tests in 2.000s\n\nOK\n"), 5)
        with self.assertRaises(RuntimeError):
            self.check("restart", "Ran 4 tests in 2.000s\n\nOK\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
