import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import shutil
import unittest


ROOT = Path(__file__).resolve().parents[1]
HELPER_PATH = ROOT / ".infra/tools/packaged_consumer_manifest.py"
SCRIPT_PATH = ROOT / ".infra/tools/verify-packaged-consumer.sh"
SPEC = importlib.util.spec_from_file_location("packaged_consumer_manifest", HELPER_PATH)
helper = importlib.util.module_from_spec(SPEC)
if SPEC.loader is not None:
    SPEC.loader.exec_module(helper)


class ManifestRewriteTests(unittest.TestCase):
    def test_rewrites_path_forms_and_preserves_manifest_semantics(self):
        package_dir = Path(tempfile.gettempdir()) / 'package "with" spaces\\folder'
        cases = [
            'path = "../../../"',
            'path="../../../"',
            "path = '../../../'",
            'path = "../../.."',
        ]
        for dependency in cases:
            with self.subTest(dependency=dependency):
                source = f'''[package]\nname = "consumer"\n\n[features]\ndefault = []\nsqlite = ["qubit-task/sqlite"]\n\n[dependencies]\nqubit-task = {{ {dependency}, version = "^0.11", default-features = false }}\nqubit-id = {{ version = "^0.7" }}\n'''
                rewritten = helper.rewrite_dependency_path(source, package_dir)
                parsed = helper.tomllib.loads(rewritten)
                self.assertEqual(parsed["dependencies"]["qubit-task"]["path"], str(package_dir.resolve()))
                original = helper.tomllib.loads(source)
                self.assertEqual(parsed["features"], original["features"])
                self.assertEqual(parsed["dependencies"]["qubit-id"], original["dependencies"]["qubit-id"])
                self.assertEqual(parsed["dependencies"]["qubit-task"]["version"], "^0.11")

    def test_rejects_missing_ambiguous_or_unsupported_dependency(self):
        cases = [
            '[dependencies]\nqubit-id = "^0.7"\n',
            '[dependencies]\nqubit-task = { version = "^0.11" }\n',
            '[dependencies]\nqubit-task = { path = "a", path = "b" }\n',
            '[dependencies]\nqubit-task = {\npath = "a"\n}\n',
            '[dev-dependencies]\nqubit-task = { path = "a" }\n',
        ]
        for source in cases:
            with self.subTest(source=source), self.assertRaises((ValueError, helper.ManifestRewriteError)):
                helper.rewrite_dependency_path(source, Path("/tmp/package"))

    def test_cli_failure_leaves_original_file_unchanged(self):
        with tempfile.TemporaryDirectory() as temporary:
            manifest = Path(temporary) / "Cargo.toml"
            original = '[dependencies]\nqubit-task = { path = "a" }\n'
            manifest.write_text(original)
            result = subprocess.run([sys.executable, str(HELPER_PATH), str(manifest), str(Path(temporary) / "missing")], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(manifest.read_text(), original)

    def test_rewrite_manifest_file_rejects_missing_package_manifest_without_writing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "consumer.toml"
            original = b'[dependencies]\nqubit-task = { path = "../../../" }\n'
            manifest.write_bytes(original)
            with self.assertRaises(helper.ManifestRewriteError):
                helper.rewrite_manifest_file(manifest, root / "missing-package")
            self.assertEqual(manifest.read_bytes(), original)

    def test_rewrite_manifest_file_rejects_wrong_package_identity_without_writing(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package_dir = root / "package"
            package_dir.mkdir()
            (package_dir / "Cargo.toml").write_text('[package]\nname = "not-qubit-task"\nversion = "0.11.0"\n')
            manifest = root / "consumer.toml"
            original = b'[dependencies]\nqubit-task = { path = "../../../" }\n'
            manifest.write_bytes(original)
            with self.assertRaises(helper.ManifestRewriteError):
                helper.rewrite_manifest_file(manifest, package_dir)
            self.assertEqual(manifest.read_bytes(), original)


class PackagedConsumerScriptTests(unittest.TestCase):
    def run_with_fake_cargo(self, mode="ok", version="0.11.0"):
        temporary = tempfile.TemporaryDirectory(prefix="packaged-consumer-test-")
        base = Path(temporary.name)
        project = base / "project"
        (project / ".infra/tools").mkdir(parents=True)
        (project / "tests/fixtures/all-features-consumer").mkdir(parents=True)
        shutil.copy2(SCRIPT_PATH, project / ".infra/tools/verify-packaged-consumer.sh")
        shutil.copy2(HELPER_PATH, project / ".infra/tools/packaged_consumer_manifest.py")
        shutil.copy2(ROOT / "tests/fixtures/all-features-consumer/Cargo.toml", project / "tests/fixtures/all-features-consumer/Cargo.toml")
        (project / "Cargo.toml").write_text(f'[package]\nname = "qubit-task"\nversion = "{version}"\n')
        fake_bin = base / "bin"
        fake_bin.mkdir()
        fake_cargo = fake_bin / "cargo"
        fake_cargo.write_text(FAKE_CARGO)
        fake_cargo.chmod(0o755)
        log = base / "cargo.log"
        env = os.environ.copy()
        env.update({"PATH": f"{fake_bin}:{env['PATH']}", "FAKE_CARGO_LOG": str(log), "FAKE_CARGO_MODE": mode, "FAKE_PACKAGE_VERSION": version})
        result = subprocess.run(["bash", str(project / ".infra/tools/verify-packaged-consumer.sh")], cwd=project, env=env, capture_output=True, text=True, timeout=30)
        return temporary, result, log.read_text() if log.exists() else ""

    def test_current_and_alternate_versions_use_isolated_targets(self):
        for version in ("0.11.0", "0.12.3"):
            with self.subTest(version=version):
                temporary, result, log = self.run_with_fake_cargo(version=version)
                try:
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertIn(f"qubit-task-{version}.crate", log)
                    self.assertIn("--target-dir", log)
                    self.assertNotIn("superpowers-rs-task-gc6n4m1u", log)
                    self.assertIn("rs-task-packaged-consumer", log)
                    self.assertIn("consumer-target", log)
                finally:
                    temporary.cleanup()

    def test_rejects_bad_archive_and_metadata_cases(self):
        for mode in ("wrong-version", "wrong-manifest", "sibling-path", "non-local-source", "missing-archive"):
            with self.subTest(mode=mode):
                temporary, result, _ = self.run_with_fake_cargo(mode=mode)
                try:
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertNotIn("Packaged all-features consumer verification passed", result.stdout)
                    if mode == "wrong-version":
                        self.assertIn("Packaged archive identity mismatch", result.stderr)
                    if mode == "non-local-source":
                        self.assertIn("non-local source", result.stderr)
                        self.assertNotIn("user:secret@", result.stdout + result.stderr)
                finally:
                    temporary.cleanup()


FAKE_CARGO = r'''#!/usr/bin/env python3
import json, os, pathlib, sys, tarfile, tempfile
from io import BytesIO

args = sys.argv[1:]
log = pathlib.Path(os.environ["FAKE_CARGO_LOG"])
with log.open("a") as stream:
    stream.write("cwd=" + os.getcwd() + " args=" + " ".join(args) + "\n")
mode = os.environ.get("FAKE_CARGO_MODE", "ok")
version = os.environ.get("FAKE_PACKAGE_VERSION", "0.11.0")
if args[0] == "package" and "--list" in args:
    print("Cargo.toml")
elif args[0] == "package":
    target = pathlib.Path(args[args.index("--target-dir") + 1])
    package_version = version
    manifest_version = "9.9.9" if mode == "wrong-version" else package_version
    archive_dir = target / "package"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive = archive_dir / f"qubit-task-{package_version}.crate"
    with log.open("a") as stream:
        stream.write("archive=" + str(archive) + "\n")
    manifest = f'[package]\nname = "qubit-task"\nversion = "{manifest_version}"\nedition = "2024"\n'
    if mode == "missing-archive":
        raise SystemExit(0)
    with tarfile.open(archive, "w:gz") as bundle:
        info = tarfile.TarInfo(f"qubit-task-{package_version}/Cargo.toml")
        data = manifest.encode()
        info.size = len(data)
        bundle.addfile(info, BytesIO(data))
elif args[0] == "generate-lockfile" or args[0] == "check":
    pass
elif args[0] == "metadata":
    manifest = pathlib.Path(args[args.index("--manifest-path") + 1]).resolve()
    import tomllib
    consumer = tomllib.loads(manifest.read_text())
    dep_path = pathlib.Path(consumer["dependencies"]["qubit-task"]["path"]).resolve()
    if mode == "wrong-manifest":
        dep_path = dep_path.parent
    task_manifest = "/sibling/qubit-task/Cargo.toml" if mode == "sibling-path" else str(dep_path / "Cargo.toml")
    task_source = "git+https://user:secret@example.test/qubit-task" if mode == "non-local-source" else None
    packages = [
        {"name": consumer["package"]["name"], "version": consumer["package"]["version"], "id": "consumer", "manifest_path": str(manifest), "source": None, "dependencies": [{"name": "qubit-task", "source": None}, {"name": "qubit-id", "source": "registry+https://github.com/rust-lang/crates.io-index"}]},
        {"name": "qubit-task", "version": version, "id": "task", "manifest_path": task_manifest, "source": task_source, "dependencies": []},
    ]
    print(json.dumps({"packages": packages, "workspace_members": ["consumer"], "resolve": {"nodes": []}}))
else:
    raise SystemExit("unexpected fake cargo command")
'''


if __name__ == "__main__":
    unittest.main()
