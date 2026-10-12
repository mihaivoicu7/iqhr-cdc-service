#!/usr/bin/env python3
"""Check changed, statically written SQL IN predicates; Python standard library only."""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

SCOPE = ("Scope: static .sql predicates only; comments, literals and quoted identifiers "
         "are ignored. Embedded application queries and dynamic SQL require review. "
         "This lexical check does not prove SQL semantics or performance.")


class CheckError(Exception):
    pass


def git(root, *args):
    try:
        result = subprocess.run(
            ["git", "--literal-pathspecs", "-C", str(root), *args], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, check=False,
        )
    except (OSError, ValueError) as exc:
        raise CheckError(str(exc)) from exc
    if result.returncode:
        raise CheckError(result.stderr.decode("utf-8", "replace").strip() or "Git command failed")
    return result.stdout


def resolve_ref(root, ref):
    return git(root, "rev-parse", "--verify", "--end-of-options", ref + "^{commit}").decode().strip()


def decode_sql(data, name):
    try:
        return data.decode("utf-16" if data.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig")
    except UnicodeError as exc:
        raise CheckError(f"{name}: expected UTF-8 or BOM-marked UTF-16 SQL: {exc}") from exc


def mask_sql(source):
    """Replace non-code with spaces, preserving offsets and all line breaks."""
    masked = list(source)
    i = 0
    while i < len(source):
        start = i
        if source.startswith("--", i):
            end = source.find("\n", i)
            i = len(source) if end < 0 else end
        elif source.startswith("/*", i):
            depth = 1
            i += 2
            while i < len(source) and depth:
                if source.startswith("/*", i):
                    depth += 1
                    i += 2
                elif source.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise CheckError("Unterminated SQL block comment")
        elif source[i] in "'\"[`":
            closing = "]" if source[i] == "[" else source[i]
            i += 1
            while i < len(source):
                if source[i] == closing:
                    if i + 1 < len(source) and source[i + 1] == closing:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            else:
                raise CheckError("Unterminated SQL literal or quoted identifier")
        else:
            i += 1
            continue
        for offset in range(start, i):
            if source[offset] not in "\r\n":
                masked[offset] = " "
    return "".join(masked)


def predicates(source):
    masked = mask_sql(source)
    stack, closing = [], {}
    for offset, char in enumerate(masked):
        if char == "(":
            stack.append(offset)
        elif char == ")" and stack:
            closing[stack.pop()] = offset
    for match in re.finditer(r"\b(?:(NOT)\s+)?(IN)\s*\(", masked, re.IGNORECASE):
        first = source.count("\n", 0, match.start()) + 1
        opening = match.end() - 1
        if opening not in closing:
            raise CheckError(f"Unbalanced {('NOT IN' if match.group(1) else 'IN')} predicate at line {first}")
        last = source.count("\n", 0, closing[opening]) + 1
        yield first, last, "NOT IN" if match.group(1) else "IN"


def display_name(name):
    if any(ord(char) < 32 or ord(char) == 127 or 0xD800 <= ord(char) <= 0xDFFF for char in name):
        return json.dumps(name, ensure_ascii=True)
    return name


def added_lines(diff):
    changed = set()
    current = None
    for line in diff.decode("utf-8", "replace").splitlines():
        hunk = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@", line)
        if hunk:
            current = int(hunk.group(1))
        elif current is not None:
            if line.startswith("+"):
                changed.add(current)
                current += 1
            elif line.startswith(" "):
                current += 1
            elif line.startswith("diff "):
                current = None
    return changed


def changed_files(root, base, head):
    args = ["diff", "--no-ext-diff", "--no-textconv", "--find-renames", "--name-status", "-z", "--diff-filter=ACMR", base]
    if head:
        args.append(head)
    records = iter(git(root, *args, "--").split(b"\0"))
    names = {}
    for status in records:
        if not status:
            continue
        old = next(records)
        new = next(records) if status.startswith((b"R", b"C")) else old
        names[new] = [old, new] if status.startswith(b"R") else [new]
    untracked = set()
    if not head:
        untracked = set(git(root, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0"))
        names.update({name: [name] for name in untracked if name})
    for raw in sorted(names):
        name = raw.decode("utf-8", "surrogateescape")
        if Path(name).suffix.lower() != ".sql":
            continue
        if head:
            source = decode_sql(git(root, "cat-file", "blob", f"{head}:{name}"), name)
        else:
            try:
                source = decode_sql((root / name).read_bytes(), name)
            except OSError as exc:
                raise CheckError(f"{name}: {exc}") from exc
        if raw in untracked:
            changed = set(range(1, source.count("\n") + 2))
        else:
            args = ["-c", "diff.interHunkContext=0", "diff", "--no-ext-diff", "--no-textconv", "--find-renames", "--no-color", "--unified=0", "--text", base]
            if head:
                args.append(head)
            paths = [value.decode("utf-8", "surrogateescape") for value in names[raw]]
            changed = added_lines(git(root, *args, "--", *paths))
        yield name, source, changed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--base", help="Compare against this commit (default: HEAD)")
    parser.add_argument("--head", help="Compare a committed head against --base")
    parser.add_argument("--files", nargs="+", type=Path, help="Fully check explicit files, relative to --root")
    options = parser.parse_args(argv)
    try:
        root = options.root.resolve(strict=True)
        if not root.is_dir():
            raise CheckError(f"{root}: root is not a directory")
        if options.files:
            if options.base or options.head:
                raise CheckError("--files cannot be combined with --base or --head")
            files = []
            for path in options.files:
                path = path if path.is_absolute() else root / path
                if path.suffix.lower() != ".sql":
                    raise CheckError(f"{path}: explicit file must have a .sql extension")
                files.append((str(path), decode_sql(path.read_bytes(), path), None))
        else:
            top = Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve()
            if top != root:
                raise CheckError(f"--root must identify the Git repository root: {top}")
            base = resolve_ref(root, options.base or "HEAD")
            head = resolve_ref(root, options.head) if options.head else None
            files = changed_files(root, base, head)
        count = 0
        for name, source, changed in files:
            try:
                matches = list(predicates(source))
            except CheckError as exc:
                raise CheckError(f"{display_name(name)}: {exc}") from exc
            for first, last, kind in matches:
                if changed is None or any(line in changed for line in range(first, last + 1)):
                    print(f"{display_name(name)}:{first}: {kind} predicate; use the project's predicate policy and review semantics")
                    count += 1
        print(SCOPE)
        print(f"{count} predicate finding(s).")
        return 1 if count else 0
    except (CheckError, OSError, ValueError) as exc:
        print(f"SQL predicate check failed: {exc}", file=sys.stderr)
        print(SCOPE, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
