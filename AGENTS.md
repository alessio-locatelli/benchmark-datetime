## Review checklist

- The benchmark architecture and setup are correct.
- Libraries are compared in an equivalent way so that their outputs (date and time parsing, conversion, or manipulation) are identical.
- Each library is used correctly, according to its documentation, and in the most efficient way available.
- Each library participates in all benchmarks that its available features support.
- Documentation (README, etc.) is in sync with the code, and the information is correct.

## How to update benchmarks

The steps are roughly as follows:

1. Find the latest released Python patch version (e.g., `3.14.7`).
2. Update the Python version in all files (`.pre-commit-config.yaml`, `mypy.ini`, `pyproject.toml`, `ruff.toml`, etc.) to use the latest patch release. Search the repository for, e.g., `3.13` and `313` to ensure there are no leftovers.
3. In `pyproject.toml`, update the minimum version for each library to the latest stable release and sync the lock.
4. Run all linters (`prek run -a`) and resolve findings, if any.
5. Read the Python files in full and confirm there is no outdated or wrong code. Update the tests if any library introduced new syntax or removed a function from its public interface.
6. Run the benchmarks as described in the README.
7. Confirm that all tests are green.
8. Confirm that there are no warnings. Any warning must be fixed in the code or suppressed with a documented rationale in the Pytest config.
9. Update the README with the new measurements.
10. Re-read the README in full and correct any outdated or wrong claims.
11. Verify the project-wide layout (e.g., with `git ls-files`) to ensure there are no outdated or unused files.
12. Commit.
