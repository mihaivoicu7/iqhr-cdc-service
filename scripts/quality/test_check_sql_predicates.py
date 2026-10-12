import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("check_sql_predicates.py")
spec = importlib.util.spec_from_file_location("checker", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class PredicateLexingTests(unittest.TestCase):
    def test_comments_literals_and_identifiers(self):
        sql = """-- IN (1)
/* NOT IN (2) /* nested IN (3) */ */
SELECT 'can''t IN (4)', N'IN (5)', "IN (6)", [odd]] IN (7)], `IN (8)`;
SELECT x WHERE x NOT /* reason */ IN
(1, 2);
"""
        self.assertEqual(list(checker.predicates(sql)), [(4, 5, "NOT IN")])

    def test_multiline_and_case(self):
        self.assertEqual(list(checker.predicates("x in\n/* c */\n(1) AND y NOT\nIN(2)")),
                         [(1, 3, "IN"), (3, 4, "NOT IN")])

    def test_nested_parentheses_and_literal_closing_parenthesis(self):
        sql = "x NOT /*comment*/\nIN (\n SELECT f(')', nested(1))\n FROM t WHERE y IN (2)\n)"
        self.assertEqual(list(checker.predicates(sql)), [(1, 5, "NOT IN"), (4, 4, "IN")])

    def test_unbalanced_membership_parentheses_fail(self):
        for sql in ["x IN (1", "x NOT IN (SELECT f(1)", "x IN ('no close)' /* comment ) */"]:
            with self.subTest(sql=sql), self.assertRaises(checker.CheckError):
                list(checker.predicates(sql))

    def test_unterminated_code_fails(self):
        for sql in ["'oops", "/*oops", "[oops", '"oops']:
            with self.subTest(sql=sql), self.assertRaises(checker.CheckError):
                list(checker.predicates(sql))


class GitFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="sql-check-")
        self.root = Path(self.temp.name)
        self.git("init", "-q")
        self.write("legacy.sql", "SELECT x FROM t WHERE x IN (1);\nSELECT 1;\n")
        self.git("add", "--", "legacy.sql")
        self.commit()

    def tearDown(self):
        self.temp.cleanup()

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.root), "-c", "user.name=Fixture",
                                        "-c", "user.email=fixture@example.invalid", *args])

    def commit(self):
        self.git("-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture")

    def write(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def check(self, *args, cwd=None, script=SCRIPT, root=True):
        command = [sys.executable, str(script)]
        if root:
            command += ["--root", str(self.root)]
        return subprocess.run(command + list(args), cwd=cwd or self.root,
                              capture_output=True, text=True)

    def assert_code(self, code, *args, **kwargs):
        result = self.check(*args, **kwargs)
        self.assertEqual(result.returncode, code, result.stdout + result.stderr)
        return result

    def test_legacy_and_unrelated_change_pass(self):
        self.assert_code(0)
        self.write("legacy.sql", "SELECT x FROM t WHERE x IN (1);\nSELECT 2;\n")
        self.assert_code(0)

    def test_new_staged_and_unstaged_predicates(self):
        self.write("legacy.sql", "SELECT x FROM t WHERE x IN (1);\nSELECT x FROM t WHERE x IN (2);\n")
        self.git("add", "--", "legacy.sql")
        self.assertIn("legacy.sql:2:", self.assert_code(1).stdout)
        self.write("legacy.sql", "SELECT x FROM t WHERE x IN (1);\nSELECT x FROM t WHERE x NOT IN (3);\n")
        self.assertIn("NOT IN", self.assert_code(1).stdout)

    def test_untracked_multiline_spans_added_line(self):
        self.write("new.sql", "SELECT x FROM t WHERE x NOT /*comment*/\nIN\n(1);\n")
        self.assertIn("new.sql:1:", self.assert_code(1).stdout)
        self.git("add", "--", "new.sql")
        self.commit()
        self.write("new.sql", "SELECT x FROM t WHERE x NOT /*comment*/\nIN\n/*new comment*/ (1);\n")
        self.assert_code(1)

    def test_changed_multiline_predicate_values_trigger(self):
        self.write("legacy.sql", "SELECT x FROM t WHERE x IN (\n1,\n2);\n")
        self.git("add", "--", "legacy.sql")
        self.commit()
        self.write("legacy.sql", "SELECT x FROM t WHERE x IN (\n1,\n3);\n")
        self.assertIn("legacy.sql:1:", self.assert_code(1).stdout)

    def test_added_not_on_prior_line_triggers(self):
        self.write("legacy.sql", "SELECT x FROM t WHERE x\nIN\n(1);\n")
        self.git("add", "--", "legacy.sql")
        self.commit()
        self.write("legacy.sql", "SELECT x FROM t WHERE x\nNOT /*added condition*/\nIN\n(1);\n")
        self.assertIn("legacy.sql:2: NOT IN", self.assert_code(1).stdout)

    def test_nested_membership_edit_triggers_outer_predicate(self):
        self.write("legacy.sql", "SELECT x FROM t WHERE x IN (\n SELECT f(')', nested(1))\n FROM other WHERE y IN (2)\n);\n")
        self.git("add", "--", "legacy.sql")
        self.commit()
        self.write("legacy.sql", "SELECT x FROM t WHERE x IN (\n SELECT f(')', nested(3))\n FROM other WHERE y IN (2)\n);\n")
        result = self.assert_code(1)
        self.assertIn("legacy.sql:1:", result.stdout)
        self.assertNotIn("legacy.sql:3:", result.stdout)

    def test_malformed_membership_is_error(self):
        self.write("new.sql", "SELECT x FROM t WHERE x NOT IN (\nSELECT f(1);\n")
        result = self.assert_code(2)
        self.assertIn("new.sql: Unbalanced NOT IN predicate at line 1", result.stderr)

    def test_spaces_unicode_and_option_like_filename(self):
        for name in ["folder with spaces/șir.sql", "--evil.sql", "odd\nname.sql", ":(exclude)other.sql", "[odd]*.sql"]:
            with self.subTest(name=name):
                self.write(name, "SELECT x FROM t WHERE x IN (1);\n")
                label = json.dumps(name) if "\n" in name else name
                self.assertIn(label + ":1:", self.assert_code(1).stdout)
                self.git("--literal-pathspecs", "add", "--", name)
                self.assertIn(label + ":1:", self.assert_code(1).stdout)
                (self.root / name).unlink()
                self.git("--literal-pathspecs", "reset", "-q", "--", name)

    def test_comments_and_strings_ignored(self):
        self.write("new.sql", "-- IN (1)\nSELECT 'NOT IN (2)', [IN (3)], \"IN (4)\";\n/* IN (5) */\n")
        self.assert_code(0)

    def test_deletion_and_pure_rename_pass(self):
        self.git("mv", "--", "legacy.sql", "renamed legacy.sql")
        self.assert_code(0)
        self.commit()
        self.git("rm", "--", "renamed legacy.sql")
        self.assert_code(0)

    def test_rename_with_new_predicate_flags(self):
        self.git("mv", "--", "legacy.sql", "renamed.sql")
        self.write("renamed.sql", "SELECT x FROM t WHERE x IN (1);\nSELECT 1;\nSELECT y FROM t WHERE y IN (2);\n")
        self.assertIn("renamed.sql:3:", self.assert_code(1).stdout)

    def test_committed_comparison_excludes_working_and_untracked(self):
        base = self.git("rev-parse", "HEAD").decode().strip()
        self.write("new.sql", "SELECT x FROM t WHERE x IN (1);\n")
        self.git("add", "--", "new.sql")
        self.commit()
        self.assert_code(1, "--base", base, "--head", "HEAD")
        self.write("ignored.sql", "SELECT x FROM t WHERE x IN (2);\n")
        self.assert_code(0, "--base", "HEAD", "--head", "HEAD")

    def test_invalid_refs_and_files_fail(self):
        self.assert_code(2, "--base", "does-not-exist")
        self.assert_code(2, "--head", "--help=evil")
        self.assert_code(2, "--files", "missing.sql")
        self.assert_code(2, "--files", "legacy.sql", "--base", "HEAD")

    def test_explicit_files_in_non_git_directory(self):
        shutil.rmtree(self.root / ".git")
        self.assert_code(1, "--files", "legacy.sql")
        self.assert_code(2)

    def test_default_root_from_distributed_location_nested_cwd(self):
        target = self.root / "scripts/quality/check_sql_predicates.py"
        target.parent.mkdir(parents=True)
        shutil.copyfile(SCRIPT, target)
        nested = self.root / "nested/deep"
        nested.mkdir(parents=True)
        self.assert_code(0, cwd=nested, script=target, root=False)
        self.write("new.sql", "SELECT x FROM t WHERE x IN (1);\n")
        self.assert_code(1, cwd=nested, script=target, root=False)

    def test_utf16_sql_addition(self):
        (self.root / "utf16.sql").write_bytes("SELECT x FROM t WHERE x IN (1);\n".encode("utf-16"))
        self.assert_code(1)
        self.git("add", "--", "utf16.sql")
        self.commit()
        (self.root / "utf16.sql").write_bytes("SELECT x FROM t WHERE x IN (1);\nSELECT y FROM t WHERE y IN (2);\n".encode("utf-16"))
        self.assertIn("utf16.sql:2:", self.assert_code(1).stdout)


if __name__ == "__main__":
    unittest.main()
