#!/usr/bin/env python3
"""Compile the installation dependencies with each README quick-start block."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib


PROJECT_ROOT = Path(__file__).resolve().parents[1]
README_FILES = (PROJECT_ROOT / "README.md", PROJECT_ROOT / "README.zh_CN.md")
EXPECTED_DEPENDENCIES = {
    "qubit-task": {"version": "0.11", "features": ["sqlite"]},
    "qubit-codec": {"version": "0.17", "features": ["registry"]},
    "qubit-id": {"version": "0.7", "features": ["serde"]},
    "qubit-model-id": "0.1",
    "serde": {"version": "1.0", "features": ["derive"]},
    "serde_json": "1.0",
    "tokio": {"version": "1.54", "features": ["macros", "rt-multi-thread", "time"]},
}


def section(text: str, heading: str) -> str:
    heading_pattern = re.compile(rf"^## {re.escape(heading)}\s*$", re.MULTILINE)
    matches = list(heading_pattern.finditer(text))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one '## {heading}' section, found {len(matches)}")
    start = matches[0].end()
    next_heading = re.search(r"^## ", text[start:], re.MULTILINE)
    end = start + next_heading.start() if next_heading else len(text)
    return text[start:end]


def fenced_blocks(text: str, language: str) -> list[str]:
    pattern = re.compile(rf"^```{re.escape(language)}\s*\n(.*?)^```\s*$", re.MULTILINE | re.DOTALL)
    return [match.group(1) for match in pattern.finditer(text)]


def extract(readme: Path, install_heading: str, quick_heading: str) -> tuple[str, dict, str]:
    text = readme.read_text(encoding="utf-8")
    install_tomls = fenced_blocks(section(text, install_heading), "toml")
    if len(install_tomls) != 1:
        raise ValueError(f"{readme}: expected one TOML dependency block, found {len(install_tomls)}")
    parsed = tomllib.loads(install_tomls[0])
    dependencies = parsed.get("dependencies")
    if not isinstance(dependencies, dict):
        raise ValueError(f"{readme}: installation TOML has no [dependencies] table")
    if dependencies != EXPECTED_DEPENDENCIES:
        raise ValueError(f"{readme}: dependencies differ from the documented complete set: {dependencies!r}")

    rust_blocks = fenced_blocks(section(text, quick_heading), "rust")
    if len(rust_blocks) != 1:
        raise ValueError(f"{readme}: expected one Rust quick-start block, found {len(rust_blocks)}")
    return install_tomls[0], dependencies, rust_blocks[0]


def local_manifest(dependency_block: str) -> str:
    manifest = dependency_block
    project_path = str(PROJECT_ROOT).replace("\\", "\\\\").replace('"', '\\"')
    model_id_path = str(PROJECT_ROOT.parent / "rs-model-metadata" / "model-id").replace("\\", "\\\\").replace('"', '\\"')
    manifest, task_count = re.subn(
        r'(?m)^qubit-task\s*=\s*\{([^}]*)\}',
        lambda match: f'qubit-task = {{ {match.group(1).rstrip().rstrip(",")}, path = "{project_path}" }}',
        manifest,
    )
    if task_count != 1:
        raise ValueError("local mode requires exactly one declared qubit-task dependency")
    manifest, model_count = re.subn(
        r'(?m)^qubit-model-id\s*=\s*"([^"]+)"\s*$',
        lambda match: f'qubit-model-id = {{ version = "{match.group(1)}", path = "{model_id_path}" }}',
        manifest,
    )
    if model_count != 1:
        raise ValueError("local mode requires exactly one declared qubit-model-id dependency")
    return manifest


def check_consumer(readme: Path, install_heading: str, quick_heading: str, mode: str, root: Path) -> None:
    dependency_block, _, source = extract(readme, install_heading, quick_heading)
    if mode == "local":
        dependency_block = local_manifest(dependency_block)
        print(f"{readme.name}: local overrides qubit-task={PROJECT_ROOT} and qubit-model-id={PROJECT_ROOT.parent / 'rs-model-metadata' / 'model-id'}")
    consumer = root / readme.stem
    (consumer / "src").mkdir(parents=True)
    manifest = "\n".join(
        [
            "[package]",
            f'name = "readme-consumer-{readme.stem.lower().replace("_", "-").replace(".", "-")}"',
            'version = "0.1.0"',
            'edition = "2024"',
            "publish = false",
            "",
            "[workspace]",
            "",
            dependency_block.strip(),
            "",
        ]
    )
    (consumer / "Cargo.toml").write_text(manifest, encoding="utf-8")
    (consumer / "src" / "main.rs").write_text(source, encoding="utf-8")
    command = ["cargo", "check", "--manifest-path", str(consumer / "Cargo.toml")]
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(root / "target")
    result = subprocess.run(command, cwd=PROJECT_ROOT, env=environment, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(
            f"{readme.name} {mode} consumer failed ({result.returncode})\n"
            f"command: {' '.join(command)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    print(f"{readme.name}: {mode} consumer cargo check passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("local", "registry"), default="local")
    args = parser.parse_args()
    headings = (("Installation", "Quick start"), ("安装", "快速开始"))
    try:
        with tempfile.TemporaryDirectory(prefix="rs-task-readme-consumer-") as temporary:
            root = Path(temporary)
            for readme, (install_heading, quick_heading) in zip(README_FILES, headings, strict=True):
                check_consumer(readme, install_heading, quick_heading, args.mode, root)
    except (OSError, ValueError, RuntimeError, tomllib.TOMLDecodeError) as error:
        print(f"README consumer check failed: {error}", file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
