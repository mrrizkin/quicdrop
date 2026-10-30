#!/bin/sh
# Release tags follow the UTC ISO week: v<YEAR>.<WEEK>.<PATCH>, where PATCH counts
# releases inside that week. Run `git fetch --tags` first on a clone that has not
# seen the release tags, otherwise the counter restarts at 0.
set -eu

prefix="v$(date -u +'%Y.%V')"

last=$(git tag -l "${prefix}.*" |
	sed -E "s/^v[0-9]{4}\.[0-9]{2}\.([0-9]+)$/\1/" |
	sort -n | tail -1)

if [ -n "$last" ]; then
	patch=$((last + 1))
else
	patch=0
fi

version="${prefix}.${patch}"
number="${version#v}"

# The version is embedded in the binaries and the bundles, so the tag has to point
# at a commit that already carries it.
stamp() {
	sed -i "s/^version = \".*\"/version = \"$number\"/" Cargo.toml
	sed -i "s/^version = \".*\"/version = \"$number\"/" apps/desktop/Cargo.toml
	sed -i "s/\"version\": \"[^\"]*\"/\"version\": \"$number\"/" apps/desktop/tauri.conf.json
	cargo metadata --format-version 1 >/dev/null
	cargo metadata --format-version 1 --manifest-path apps/desktop/Cargo.toml >/dev/null
}

case "${1:-}" in
"")
	echo "$version"
	;;
--create)
	if [ -n "$(git status --porcelain)" ]; then
		echo "working tree is not clean; commit or stash first" >&2
		exit 1
	fi
	if git rev-parse -q --verify "refs/tags/$version" >/dev/null; then
		echo "$version is already tagged" >&2
		exit 1
	fi
	stamp
	if [ -n "$(git status --porcelain)" ]; then
		git add Cargo.toml Cargo.lock apps/desktop/Cargo.toml apps/desktop/Cargo.lock \
			apps/desktop/tauri.conf.json
		git commit -q -m "chore: release $number"
	fi
	git tag -a "$version" -m "$version"
	echo "$version"
	echo "push it with: git push origin $version" >&2
	;;
*)
	echo "usage: $0 [--create]" >&2
	exit 2
	;;
esac
