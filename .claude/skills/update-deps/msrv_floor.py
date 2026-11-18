"""Print the highest `rust-version` among the dependencies cargo resolved.

Two numbers, because they bind different people: normal and build dependencies set the
floor for users of the published crates, and dev dependencies additionally set it for
`just msrv`, which checks every target.

Usage: python3 .claude/skills/update-deps/msrv_floor.py   (from the repository root)
"""

import json
import subprocess


def version(text):
    return tuple(int(part) for part in (text or "0").split("."))


def main():
    metadata = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--all-features"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    packages = {p["id"]: p for p in metadata["packages"]}
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    members = set(metadata["workspace_members"])

    def reachable(include_dev):
        seen, stack = set(), list(members)
        while stack:
            for dep in nodes[stack.pop()]["deps"]:
                kinds = {k["kind"] for k in dep["dep_kinds"]}
                wanted = kinds - {"dev"} or (kinds if include_dev else set())
                if dep["pkg"] in members or not wanted or dep["pkg"] in seen:
                    continue
                seen.add(dep["pkg"])
                stack.append(dep["pkg"])
        return seen

    for label, include_dev in (("published crates", False), ("all targets", True)):
        floor = max(
            (version(packages[i]["rust_version"]), packages[i]["name"])
            for i in reachable(include_dev)
        )
        print(f"{label:16} needs rust {'.'.join(map(str, floor[0]))} ({floor[1]})")


if __name__ == "__main__":
    main()
