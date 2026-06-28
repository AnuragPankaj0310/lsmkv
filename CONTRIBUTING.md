# CONTRIBUTING.md

# Contributing to LSMKV

Thank you for your interest in contributing to **LSMKV**.

This project is an educational implementation of a distributed LSM-tree key-value store. Contributions that improve correctness, documentation, testing, or maintainability are welcome.

---

# Getting Started

## 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/lsmkv.git

cd lsmkv
```

---

## 2. Create a Virtual Environment

### Windows

```powershell
python -m venv .venv

.\.venv\Scripts\Activate.ps1
```

### Linux / macOS

```bash
python3 -m venv .venv

source .venv/bin/activate
```

---

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 4. Run the Test Suite

```bash
pytest -v
```

All tests should pass before submitting changes.

---

# Development Workflow

1. Create a feature branch.

```bash
git checkout -b feature/my-feature
```

2. Make your changes.

3. Run the test suite.

```bash
pytest -v
```

4. Commit your work.

```bash
git commit -m "Add my feature"
```

5. Push the branch.

```bash
git push origin feature/my-feature
```

6. Open a Pull Request.

---

# Coding Guidelines

Please follow these conventions:

* Follow PEP 8.
* Prefer type hints for new code.
* Keep functions focused and small.
* Add docstrings to public APIs.
* Avoid unnecessary dependencies.
* Use logging instead of `print()` for diagnostics.

---

# Testing

Whenever possible, add tests for new functionality.

Tests are located in:

```text
tests/
```

Run all tests before opening a Pull Request:

```bash
pytest -v
```

---

# Documentation

If your contribution changes user-facing behavior, please update the relevant documentation:

* README.md
* ARCHITECTURE.md
* BENCHMARK.md
* CHANGELOG.md

If new CLI commands or metrics are added, update the documentation accordingly.

---

# Reporting Issues

If you discover a bug, please include:

* Operating system
* Python version
* Docker version (if applicable)
* Steps to reproduce
* Expected behavior
* Actual behavior
* Error messages or logs

Providing a minimal reproducible example is appreciated.

---

# Feature Requests

Feature requests are welcome. Before proposing a major change, consider opening an issue to discuss the design and implementation approach.

---

# Code of Conduct

Please be respectful and constructive in all interactions.

The goal of this project is to provide a clean, educational implementation of a distributed LSM-tree storage engine while fostering a positive learning environment.

---

# License

By contributing, you agree that your contributions will be licensed under the same MIT License as the project.
