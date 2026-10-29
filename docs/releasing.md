# Release Process

This project uses [Semantic Versioning](https://semver.org/) and GitHub Releases.

## Version Format

`vMAJOR.MINOR.PATCH` — e.g., `v0.1.0`, `v0.2.0`, `v1.0.0`

- **MAJOR**: Breaking API changes (e.g., `/api/v1/` → `/api/v2/`)
- **MINOR**: New features, backward-compatible (e.g., new page, new data source)
- **PATCH**: Bug fixes, performance improvements

## How to Release

### 1. Update the changelog

Edit `CHANGELOG.md` and add a section for the new version (keep an empty
`## [Unreleased]` heading above it). An optional ` — Title` after the date becomes
the GitHub Release title (`v0.2.0 — Title`); without it the title is just the tag.

```markdown
## [0.2.0] - 2026-05-01 — Short release title

### Added
- New feature X
- Support for Y

### Fixed
- Bug in Z
```

### 2. Commit the changelog

```bash
git add CHANGELOG.md
git commit -m "Prepare release v0.2.0"
git push origin main
```

### 3. Create and push a tag

```bash
git tag v0.2.0
git push origin v0.2.0
```

This triggers the GitHub Actions release workflow (`.github/workflows/release.yml`), which:
- Extracts the changelog section for this version (the job fails if it's missing or empty)
- Creates a GitHub Release with that section as the release notes, marked **Latest**
- Marks pre-releases automatically for `-rc`, `-beta` or `-alpha` tags
- Leaves an already-existing release for the tag unchanged (safe to re-run)

### 4. Verify

Check the [Releases page](../../releases) on GitHub to confirm the release was created.

## Pre-releases

For release candidates:

```bash
git tag v0.2.0-rc1
git push origin v0.2.0-rc1
```

These are automatically marked as pre-releases on GitHub.
