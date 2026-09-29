## Adding a new library to benchmarks

1. Add it to dependencies via `uv add`.
2. Investigate the library's documentation and repository for any existing benchmarks. If available, use them for inspiration and to understand best practices (the library authors are likely best positioned to know how to use the library correctly and efficiently).
3. Identify all benchmarks (date and time parsing, manipulation, dumping, etc.) where the library can participate.
4. Can we add new tests for date and time parsing, dumping, or manipulation? For example, look for practical usage cases that exist but have never been tested.
5. Extend and update tests.
6. Refer to the section on updating the benchmarks report to refresh the histograms and README.

Final Report:

- New tests (if any; if none, state "no new tests").
- Whether the library publishes its own benchmarks and whether they align with our results.
- Where the library ranks compared to alternatives based on our benchmarks.
- List the tests that were extended and the tests where the library could not be added.

---

## Review checklist

- The benchmark architecture and setup are correct.
- Libraries are compared in an equivalent way so that their outputs (date and time parsing, conversion, or manipulation) are identical.
- Each library is used correctly, according to its documentation, and in the most efficient way available.
- Each library participates in all benchmarks that its available features support.
- Documentation (README, etc.) is in sync with the code, and the information is correct.

---

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
