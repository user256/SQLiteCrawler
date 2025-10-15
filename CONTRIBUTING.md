# Contributing to SQLiteCrawler

Thank you for your interest in contributing to SQLiteCrawler! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites

- Python 3.9 or higher
- Git
- Basic understanding of web crawling concepts
- Familiarity with SQLite and async Python

### Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/SQLiteCrawler.git
   cd SQLiteCrawler
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies**
   ```bash
   pip install -e .
   pip install -e ".[js]"  # Include JavaScript rendering support
   ```

4. **Install development tools**
   ```bash
   pip install pytest black isort mypy flake8
   ```

## 🛠️ Development Workflow

### Branch Strategy

- `main` - Production-ready code
- `develop` - Integration branch for features
- `feature/description` - New features
- `bugfix/description` - Bug fixes
- `hotfix/description` - Critical fixes

### Making Changes

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Follow the coding standards below
   - Add tests for new functionality
   - Update documentation as needed

3. **Test your changes**
   ```bash
   # Run tests
   pytest
   
   # Run linting
   black --check src/
   isort --check-only src/
   flake8 src/
   mypy src/
   ```

4. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: add new feature description"
   ```

5. **Push and create a pull request**
   ```bash
   git push origin feature/your-feature-name
   ```

## 📝 Code Standards

### Python Style

We use the following tools for code formatting and linting:

- **Black** - Code formatting
- **isort** - Import sorting
- **flake8** - Linting
- **mypy** - Type checking

Run these before committing:
```bash
black src/
isort src/
flake8 src/
mypy src/
```

### Code Style Guidelines

1. **Use type hints** for all function parameters and return values
2. **Follow PEP 8** naming conventions
3. **Use descriptive variable names**
4. **Add docstrings** for all public functions and classes
5. **Keep functions focused** - one responsibility per function
6. **Use async/await** for I/O operations
7. **Handle exceptions** appropriately

### Example Code Style

```python
async def fetch_url(url: str, config: HttpConfig) -> Optional[Dict[str, Any]]:
    """
    Fetch a URL and return response data.
    
    Args:
        url: The URL to fetch
        config: HTTP configuration settings
        
    Returns:
        Response data dictionary or None if failed
        
    Raises:
        HttpError: If the request fails
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, **config.to_dict()) as response:
                return await process_response(response)
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src/sqlitecrawler

# Run specific test file
pytest tests/test_crawl.py

# Run with verbose output
pytest -v
```

### Writing Tests

- Place tests in the `tests/` directory
- Use descriptive test names
- Test both success and failure cases
- Mock external dependencies (HTTP requests, file I/O)
- Use fixtures for common test data

### Test Structure

```python
import pytest
from unittest.mock import AsyncMock, patch
from sqlitecrawler.crawl import crawl

class TestCrawl:
    @pytest.mark.asyncio
    async def test_crawl_success(self):
        """Test successful crawl operation."""
        # Arrange
        url = "https://example.com"
        
        # Act
        result = await crawl(url)
        
        # Assert
        assert result is not None
        assert result["status"] == "success"
    
    @pytest.mark.asyncio
    async def test_crawl_invalid_url(self):
        """Test crawl with invalid URL."""
        # Arrange
        url = "not-a-url"
        
        # Act & Assert
        with pytest.raises(ValueError):
            await crawl(url)
```

## 📚 Documentation

### Code Documentation

- **Docstrings**: Use Google-style docstrings for all public functions
- **Comments**: Explain complex logic and business rules
- **Type hints**: Always include type annotations

### User Documentation

- **README.md**: Keep up to date with new features
- **ROADMAP.md**: Update with completed and planned features
- **Examples**: Add usage examples for new features

## 🐛 Bug Reports

When reporting bugs, please include:

1. **Environment details**
   - Python version
   - Operating system
   - SQLiteCrawler version

2. **Steps to reproduce**
   - Clear, numbered steps
   - Sample URLs or data if applicable

3. **Expected vs actual behavior**
   - What you expected to happen
   - What actually happened

4. **Error messages and logs**
   - Full error tracebacks
   - Relevant log output

### Bug Report Template

```markdown
**Bug Description**
Brief description of the bug.

**Environment**
- Python: 3.11.0
- OS: Ubuntu 22.04
- SQLiteCrawler: 0.3.0

**Steps to Reproduce**
1. Run command: `python main.py https://example.com`
2. Observe error: [error message]

**Expected Behavior**
[What should happen]

**Actual Behavior**
[What actually happens]

**Additional Context**
[Any other relevant information]
```

## 💡 Feature Requests

When requesting features, please include:

1. **Problem description**
   - What problem does this solve?
   - Who would benefit from this feature?

2. **Proposed solution**
   - How should the feature work?
   - Any design considerations?

3. **Alternatives considered**
   - Other ways to solve the problem
   - Why this approach is preferred

## 🔄 Pull Request Process

### Before Submitting

1. **Ensure tests pass**
   ```bash
   pytest
   ```

2. **Run code quality checks**
   ```bash
   black --check src/
   isort --check-only src/
   flake8 src/
   mypy src/
   ```

3. **Update documentation**
   - README.md if needed
   - Docstrings for new functions
   - Type hints for all code

4. **Update version** (if applicable)
   - Update version in `pyproject.toml`
   - Update version in `src/sqlitecrawler/__init__.py`

### Pull Request Template

```markdown
## Description
Brief description of changes.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Tests pass locally
- [ ] New tests added for new functionality
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] No breaking changes (or clearly documented)

## Related Issues
Closes #123
```

## 🏗️ Architecture Guidelines

### Database Design

- **Normalize data** - Use proper foreign keys and relationships
- **Index appropriately** - Add indexes for frequently queried columns
- **Version migrations** - Use migration functions for schema changes
- **Backup compatibility** - Ensure new versions can read old databases

### Async Programming

- **Use async/await** for I/O operations
- **Avoid blocking calls** in async functions
- **Handle exceptions** properly in async contexts
- **Use proper concurrency limits** to avoid overwhelming servers

### Error Handling

- **Fail gracefully** - Don't crash on individual URL failures
- **Log appropriately** - Use appropriate log levels
- **Provide useful error messages** - Help users understand what went wrong
- **Retry logic** - Implement exponential backoff for transient failures

## 📋 Commit Message Convention

We use conventional commits for clear commit history:

```
type(scope): description

[optional body]

[optional footer]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### Examples

```
feat(crawl): add JavaScript rendering support
fix(db): resolve database locking issue
docs(readme): update installation instructions
test(crawl): add tests for redirect handling
```

## 🤝 Community Guidelines

### Code of Conduct

- **Be respectful** - Treat everyone with respect
- **Be constructive** - Provide helpful feedback
- **Be patient** - Remember that contributors are volunteers
- **Be inclusive** - Welcome contributors from all backgrounds

### Getting Help

- **GitHub Issues** - For bugs and feature requests
- **GitHub Discussions** - For questions and general discussion
- **Pull Requests** - For code contributions

## 🎯 Areas for Contribution

### High Priority

- **Performance improvements** - Faster crawling, better memory usage
- **Error handling** - More robust error recovery
- **Documentation** - Better examples and tutorials
- **Testing** - More comprehensive test coverage

### Medium Priority

- **New features** - Additional analysis capabilities
- **Database optimizations** - Better query performance
- **UI improvements** - Better command-line interface
- **Integration** - Support for more data formats

### Low Priority

- **Code cleanup** - Refactoring large functions
- **Style improvements** - Better code organization
- **Tooling** - Development and deployment tools

## 📞 Contact

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and community discussion

Thank you for contributing to SQLiteCrawler! 🚀
