# DSS150P - Laboratory Activity 3 Report

## Goal 1: Reproducible Data Engineering Environments

### Task A - Build and verify a virtual environment

**1. Record the Python version and selected package versions:**
*   Python version: Python 3.11+ (Local environment)
*   `pandas==2.2.3`
*   `pyarrow==17.0.0`
*   `psycopg[binary]==3.2.3`
*   `python-dotenv==1.0.1`
*   `PyYAML==6.0.2`

**2. Explain why the virtual environment should not be committed to Git:**
*   **Platform Dependency:** Virtual environments contain binaries, compiled libraries, and executable scripts (like the Python interpreter itself) that are tied to the specific operating system and architecture they were created on (e.g., Windows x64 vs. macOS ARM). A `.venv` created on Windows will not work on Linux or macOS.
*   **Absolute Paths:** Virtual environments frequently hard-code absolute file paths to the local machine, which break as soon as the project is cloned into a different directory on another machine.
*   **Repository Bloat:** The `.venv` directory contains thousands of files and takes up significant disk space. Version control is meant for source code, not large binary dependencies. 
*   **Reproducibility:** The industry standard for reproducibility is to commit a dependency manifest (like `requirements.txt`) so that any user can reliably build a fresh, matching environment on their own system.
