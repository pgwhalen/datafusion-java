---
name: release
description: Run pre-release checks (formatting, javadoc, docs), auto-fix trivial issues, and trigger the publish-ffi workflow. Use when the user asks to release, publish, cut a release, or ship.
disable-model-invocation: true
allowed-tools: Read, Grep, Glob, Edit, Write, Bash, Agent
---

# Release

Run local pre-release checks, fix trivial issues, and trigger the CI release workflow.

Read `.claude/rules/build-and-release.md` for full release context.

## Step 0: Set JAVA_HOME to JDK 22

CI uses JDK 22 (temurin). **All Gradle commands in this skill MUST use JDK 22.** Set `JAVA_HOME` before every Gradle invocation:

```bash
export JAVA_HOME=/Users/pgwhalen/Library/Java/JavaVirtualMachines/temurin-22.0.2/Contents/Home
```

**Do NOT use JDK 21 or JDK 25** — JDK 21 has different `{@snippet}` behavior than JDK 22, and JDK 25 is incompatible with Gradle 8.10. If JDK 22 is not installed at that path, stop and tell the user to install it.

## Step 1: Read the current version

Read `datafusion-ffi-java/build.gradle` and extract the current SNAPSHOT version. Confirm it ends with `-SNAPSHOT`. Print the version that will be released (with `-SNAPSHOT` stripped).

## Step 2: Run local pre-release checks

These mirror the `checks` job in `.github/workflows/publish-ffi.yml` and catch issues before the expensive multi-platform CI build.

### 2a: Generate Diplomat bindings

```bash
cd datafusion-ffi-native && cargo check
```

If this fails, stop and report — these are not trivially fixable.

### 2b: Formatting (spotlessCheck)

Run:
```bash
./gradlew :datafusion-ffi-java:spotlessCheck -PskipDiplomatGeneration
```

If it fails, **auto-fix** by running:
```bash
./gradlew :datafusion-ffi-java:spotlessApply -PskipDiplomatGeneration
```

Then commit the formatting fix (message: `Fix formatting for release`).

### 2c: Javadoc

Run:
```bash
./gradlew :datafusion-ffi-java:javadoc -PskipDiplomatGeneration
```

If it fails, read the error output and fix trivial javadoc issues (missing `@param`, malformed tags, etc.). Re-run until it passes or the issues are non-trivial. Commit any fixes (message: `Fix javadoc for release`).

### 2d: Documentation link verification

Run:
```bash
./gradlew :datafusion-ffi-java:verifyDocsHttp -PskipDiplomatGeneration
```

If it fails, read the error output and fix broken links or references. Commit any fixes (message: `Fix doc links for release`).

## Step 3: Run tests

```bash
./gradlew :datafusion-ffi-java:test
```

If tests fail, stop and report — do not auto-fix test failures.

## Step 4: Trigger the release workflow

All checks passed. Trigger the workflow:

```bash
gh workflow run publish-ffi.yml --ref $(git branch --show-current) -R pgwhalen/datafusion-java
```

Report success and the version being released. Remind the user they can monitor the workflow with:
```
gh run list -w publish-ffi.yml -R pgwhalen/datafusion-java --limit 1
```
