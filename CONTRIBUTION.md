<a name="readme-top"></a>
# Indexify 

![Tests](https://github.com/tensorlakeai/indexify/actions/workflows/test.yaml/badge.svg?branch=main)
[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)

## Contributing to Indexify

Thank you for considering contributing to Indexify! We appreciate your efforts to help improve this project. This guide will walk you through the steps to contribute, from setting up the server to writing tests and submitting your work.

## 1. Getting Started

To start contributing, please follow these steps:

1. **Fork the repository** and clone it to your local machine:

```bash
git clone https://github.com/<your-username>/indexify.git
```

2. **Install dependencies**:

```bash
pip install -r requirements.txt
```

3. **Create a new branch** for your feature or bugfix:

```bash
git checkout -b feature/your-feature-name
```

4. **Run the Server Locally**: You can run the server locally to test your changes:

```bash
python -m indexify.server
```

This will start the local development server. Check the logs to ensure it’s running correctly.

## 2. Writing Tests

We encourage all contributors to write tests for any new features or bug fixes. This ensures the stability and quality of the codebase.

**2.1 Unit Tests**
* Unit tests should cover individual components or functions.
* We use `pytest` as our testing framework. Make sure it is installed:
```bash
pip install pytest
```
* Tests should be added in the `tests/` directory, and each test file should follow the naming convention` test_<module_name>.py`.

**Example of a Basic Unit Test:**
```python
def test_function():
    assert function_name(args) == expected_output
```

* Run unit tests with:
```bash
pytest
```

## 3. Running Integration Tests

Integration tests ensure that the overall system works as expected. For Indexify, this means validating multi-stage workflows and API endpoints.

**3.1 Running `graph_behaviours.py`**
The integration tests are located in `tests/integration/graph_behaviours.py`. To run these tests, follow the steps below:

1. **Ensure the server is running:**
```bash
python -m indexify.server
```
2. **Run the integration tests:**
```bash
pytest tests/integration/graph_behaviours.py
```

This test suite will simulate various real-world graph workflows, ensuring that data flows correctly between different nodes and that API requests return expected results.

## 4. Writing a Test Plan

Whenever you implement a feature or fix a bug, it’s essential to document how you’ve tested the functionality. A good test plan outlines the steps you took to verify your changes.

**4.1 What to Include in a Test Plan:**

* **Feature description**: What functionality are you testing?
* **Test cases**: A list of all edge cases and scenarios you tested. Include both positive (expected behavior) and negative (failure conditions) tests.
* **Tools and frameworks**: Mention any libraries, scripts, or external services used in testing (e.g., Postman for API tests, pytest for unit testing).
* **Environment**: Specify the setup in which the feature was tested (local server, test environment, specific configurations).
* **Expected Results**: What should the feature output under various conditions?

**Example Test Plan**:

```markdown
### Feature: User Login API

#### Test Cases:
1. Valid login request (status code 200)
2. Invalid login with incorrect credentials (status code 401)
3. Request with missing fields (status code 400)

#### Tools Used:
- Postman for API requests
- Pytest for unit tests

#### Environment:
- Local server running on port 8000
- Python 3.9

#### Expected Results:
- Successful login returns JWT token and status code 200
- Failed login due to incorrect credentials returns status code 401
```

Once your tests are written and verified, commit your changes and submit a pull request.

## 5. Submitting a Pull Request (PR)

1. Push your changes to your fork:
```bash
git push origin feature/your-feature-name
```
2. Create a pull request from your forked repository. In your pull request:
* **Describe your changes** clearly and concisely.
* **Link any related issues**.
* **Attach your test plan** and mention the tests that were run to verify your changes.
3. One of the maintainers will review your PR, provide feedback, and request any necessary changes.

## 6. Additional Guidelines

* Keep your commits small and focused on one change at a time.
* Write meaningful commit messages. For example: `Add unit tests for login API`.
* Ensure your code follows the project's style guide.
* Always test your changes before submitting.

We appreciate your contributions and look forward to collaborating with you!