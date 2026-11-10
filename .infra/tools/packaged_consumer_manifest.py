#!/usr/bin/env python3
"""Safely point a copied consumer manifest at an unpacked crate."""

import argparse
import json
from pathlib import Path
import re
import sys
import tomllib


class ManifestRewriteError(ValueError):
    """The consumer manifest does not have the supported dependency shape."""


def _inline_table_fields(body: str):
    """Yield start/end offsets for inline table fields, splitting outside values."""
    start = 0
    quote = None
    escaped = False
    depth = 0
    for index, char in enumerate(body):
        if quote:
            if quote == '"' and char == "\\" and not escaped:
                escaped = True
                continue
            if char == quote and not escaped:
                quote = None
            escaped = False
            continue
        if char in ('"', "'"):
            quote = char
        elif char in "[{":
            depth += 1
        elif char in "]}":
            depth -= 1
        elif char == "," and depth == 0:
            yield start, index
            start = index + 1
    yield start, len(body)


def rewrite_dependency_path(text: str, package_dir: Path) -> str:
    """Rewrite only the inline qubit-task path under [dependencies]."""
    try:
        parsed = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise ManifestRewriteError(f"invalid consumer TOML: {error}") from error

    dependencies = parsed.get("dependencies")
    if not isinstance(dependencies, dict):
        raise ManifestRewriteError("consumer manifest has no [dependencies] table")
    dependency = dependencies.get("qubit-task")
    if not isinstance(dependency, dict):
        raise ManifestRewriteError("[dependencies].qubit-task must be an inline table")
    if not isinstance(dependency.get("path"), str):
        raise ManifestRewriteError("[dependencies].qubit-task must contain a string path")

    lines = text.splitlines(keepends=True)
    section_start = None
    section_end = len(lines)
    for index, line in enumerate(lines):
        header = re.match(r"^\s*\[([^\]]+)\]\s*(?:#.*)?(?:\r?\n)?$", line)
        if not header:
            continue
        if section_start is not None:
            section_end = index
            break
        if header.group(1).strip() == "dependencies":
            section_start = index + 1
    if section_start is None:
        raise ManifestRewriteError("[dependencies].qubit-task must use an inline table")

    candidates = []
    for index in range(section_start, section_end):
        line = lines[index]
        assignment = re.match(r'^\s*(?:"qubit-task"|qubit-task)\s*=\s*(.*?)(?:\r?\n)?$', line)
        if assignment:
            candidates.append((index, assignment.group(1)))
    if len(candidates) != 1:
        raise ManifestRewriteError("expected exactly one qubit-task entry in [dependencies]")

    index, value = candidates[0]
    inline = re.match(r"^\{(.*)\}\s*(?:#.*)?$", value)
    if not inline:
        raise ManifestRewriteError("[dependencies].qubit-task must be a single-line inline table")
    body = inline.group(1)
    path_fields = []
    for field_start, field_end in _inline_table_fields(body):
        field = body[field_start:field_end]
        path_field = re.match(r"\s*(?:path|\"path\"|'path')\s*=\s*", field)
        if path_field:
            path_fields.append((field_start, field_end, path_field.end()))
    if len(path_fields) != 1:
        raise ManifestRewriteError("expected exactly one path field in qubit-task inline table")
    field_start, field_end, value_start = path_fields[0]
    field = body[field_start:field_end]
    token = re.match(r'''(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')\s*$''', field[value_start:])
    if token is None:
        raise ManifestRewriteError("qubit-task path must be a single-line TOML string")

    replacement = json.dumps(str(package_dir.resolve()), ensure_ascii=False)
    start = field_start + value_start + token.start()
    end = field_start + value_start + token.end()
    rewritten_value = body[:start] + " " + replacement + body[end:]
    rewritten_line = lines[index]
    line_start = rewritten_line.index(value)
    lines[index] = rewritten_line[:line_start] + "{" + rewritten_value + "}" + ("\n" if rewritten_line.endswith("\n") else "")
    result = "".join(lines)

    try:
        reparsed = tomllib.loads(result)
    except tomllib.TOMLDecodeError as error:
        raise ManifestRewriteError(f"rewritten consumer TOML is invalid: {error}") from error
    actual = reparsed["dependencies"]["qubit-task"].get("path")
    expected = str(package_dir.resolve())
    if actual != expected:
        raise ManifestRewriteError(f"rewritten dependency path did not resolve to {expected!r}")
    return result


def rewrite_manifest_file(manifest_path: Path, package_dir: Path) -> None:
    if not manifest_path.is_file():
        raise ManifestRewriteError(f"consumer manifest does not exist: {manifest_path}")
    package_manifest = package_dir / "Cargo.toml"
    if not package_manifest.is_file():
        raise ManifestRewriteError(f"unpacked package Cargo.toml does not exist: {package_manifest}")
    try:
        package = tomllib.loads(package_manifest.read_text(encoding="utf-8")).get("package", {})
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ManifestRewriteError(f"cannot parse unpacked package Cargo.toml: {error}") from error
    if package.get("name") != "qubit-task" or not isinstance(package.get("version"), str):
        raise ManifestRewriteError("unpacked package identity must be qubit-task with a string version")
    source = manifest_path.read_text(encoding="utf-8")
    rewritten = rewrite_dependency_path(source, package_dir)
    manifest_path.write_text(rewritten, encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("package_dir", type=Path)
    arguments = parser.parse_args(argv)
    try:
        rewrite_manifest_file(arguments.manifest, arguments.package_dir)
    except (ManifestRewriteError, OSError) as error:
        print(f"Cannot rewrite packaged consumer manifest: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
