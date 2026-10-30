# AUR recipes

Two packages, both installable with your favourite AUR helper:

| Directory     | Package        | Source                                              |
| ------------- | -------------- | --------------------------------------------------- |
| `quicdrop-bin`| prebuilt       | release assets: the CLI tarball and the `.deb` for the desktop binary |
| `quicdrop`    | source build   | `v<pkgver>` archive from GitHub, built with cargo    |

`quicdrop-bin` installs `qd`, `quicdropd`, `quicdrop-desktop`, the desktop entry and
the icons. `quicdrop` builds all three from source and needs the Tauri build deps.

## Publishing

AUR keeps one git repository per package, so these directories become two
repositories. First replace the `# Maintainer:` line in each `PKGBUILD` with your
name and email — AUR requires it.

```bash
cd packaging/aur/quicdrop-bin
makepkg --printsrcinfo > .SRCINFO
git init -q
git remote add origin ssh://aur@aur.archlinux.org/quicdrop-bin.git
git add PKGBUILD .SRCINFO
git commit -m "quicdrop-bin 2026.44.9-1"
git push origin master
```

The same for `quicdrop`.

## Updates

For each release, bump `pkgver` and refresh the checksums:

```bash
updpkgsums                      # rewrites sha256sums from the sources
makepkg --printsrcinfo > .SRCINFO
```

The `quicdrop` archive checksum comes from
`https://github.com/<owner>/quicdrop/archive/v<pkgver>.tar.gz`. GitHub has
regenerated these archives before; if the sum stops matching, re-run `updpkgsums`
against a fresh download rather than pinning the old value.

## Testing a recipe

```bash
makepkg -f          # builds the package in place
pacman -Qlp *.pkg.tar.zst
```

`pkg/`, `src/` and the built `*.pkg.tar.zst` are ignored by git.
