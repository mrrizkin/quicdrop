# AUR recipes

Two packages:

| Directory      | Package      | Source                                                           |
| -------------- | ------------ | ---------------------------------------------------------------- |
| `quicdrop-bin` | prebuilt     | the release `.deb`, which carries the CLI and the desktop binary |
| `quicdrop`     | source build | `v<pkgver>` archive from GitHub                                  |

`quicdrop-bin` installs `qd`, `quicdropd`, `quicdrop-desktop`, the desktop entry and
the icons. `quicdrop` builds all three from source and needs the Tauri build deps.

## Publishing

AUR keeps one git repository per package. Replace the `# Maintainer:` line in each
`PKGBUILD` with your name and email first; AUR requires it.

```bash
cd packaging/aur/quicdrop-bin
makepkg --printsrcinfo > .SRCINFO
git init -q
git remote add origin ssh://aur@aur.archlinux.org/quicdrop-bin.git
git add PKGBUILD .SRCINFO
git commit -m "quicdrop-bin <version>-1"
git push origin master
```

The same for `quicdrop`.

## Updates

`just tag` stamps `packaging/arch/PKGBUILD`; these AUR recipes are updated after the
release assets exist, because their checksums come from published artifacts.

```bash
updpkgsums                      # rewrites sha256sums from the sources
makepkg --printsrcinfo > .SRCINFO
```

`quicdrop-bin` sources are `quicdrop-desktop-<version>-amd64.deb` (three binaries,
desktop entry, icons) and the source archive (licence text). A renamed asset makes
`updpkgsums` fail on the missing URL, so check both against the release page.

## Testing a recipe

```bash
makepkg -f          # builds the package in place
pacman -Qlp *.pkg.tar.zst
```

`pkg/`, `src/` and the built `*.pkg.tar.zst` are ignored by git.
