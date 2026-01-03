# GitHub Actions Workflows

This directory contains modern CI/CD workflows for the PyErgon project following 2025 best practices.

## Workflows

### 🔧 CI Workflow (`ci.yml`)

Runs on every push and pull request to main/develop branches.

**Jobs:**
- **Lint**: Code style checking with Ruff (check + format)
- **Type Check**: Static type analysis with mypy
- **Test**: Runs test suite across Python 3.11 and 3.12
  - Generates coverage reports
  - Uploads coverage to Codecov
- **Test Redis**: Integration tests with Redis backend
- **Security**: Dependency vulnerability scanning with pip-audit
- **Build**: Package building and validation

**Features:**
- ✅ Concurrency control (cancels outdated runs)
- ✅ uv caching for fast dependency installation
- ✅ Matrix testing across Python versions
- ✅ Redis service container for integration tests
- ✅ Artifact upload for build outputs

### 📦 Publish Workflow (`publish.yml`)

Deploys packages to PyPI using trusted publishing (no credentials needed).

**Triggers:**
- Automatic: On GitHub releases
- Manual: Workflow dispatch with environment selection (PyPI or TestPyPI)

**Jobs:**
- **Build**: Creates distribution packages (wheel + sdist)
- **Publish to PyPI**: Publishes to production PyPI
- **Publish to TestPyPI**: Publishes to test PyPI
- **GitHub Release**: Signs distributions and uploads to GitHub release

**Features:**
- ✅ Trusted publishing (OIDC authentication)
- ✅ Sigstore signing for supply chain security
- ✅ Manual deployment option for testing
- ✅ Artifact preservation across jobs

### 🔒 CodeQL Security Scan (`codeql.yml`)

Automated security vulnerability scanning.

**Triggers:**
- Push/PR to main/develop
- Weekly schedule (Mondays at midnight UTC)

**Features:**
- ✅ GitHub Advanced Security integration
- ✅ Security-and-quality query pack
- ✅ Automated vulnerability detection

### 📊 Dependency Review (`dependency-review.yml`)

Reviews dependency changes in pull requests.

**Features:**
- ✅ Automatic PR comments with dependency analysis
- ✅ Fails on moderate+ severity vulnerabilities
- ✅ License compliance checking

## Dependabot Configuration

Automated dependency updates configured in `dependabot.yml`:

- **GitHub Actions**: Weekly updates on Mondays
- **Python packages**: Weekly updates with grouped dev dependencies
- **Auto-labeling**: Automatic labels for easy filtering
- **Conventional commits**: Prefixed commit messages (ci:, deps:)

## Modern Best Practices Applied

Based on research from:
- [GitHub Actions for Python (2025)](https://ber2.github.io/posts/2025_github_actions_python/)
- [uv GitHub Actions Guide](https://docs.astral.sh/uv/guides/integration/github/)
- [CI/CD Optimization Guide](https://medium.com/@george_bakas/optimizing-your-ci-cd-github-actions-a-comprehensive-guide-f25ea95fd494)

### ✅ Performance Optimizations

1. **Smart Caching**
   - uv cache with `cache-dependency-glob: "uv.lock"`
   - Reuses cache across all jobs
   - Invalidates on lockfile changes

2. **Concurrency Control**
   - Cancels outdated workflow runs
   - Prevents resource waste

3. **Parallel Jobs**
   - Lint, typecheck, and security run independently
   - Matrix strategy for multi-version testing

### ✅ Security Best Practices

1. **Trusted Publishing**
   - No PyPI tokens in secrets
   - OIDC-based authentication
   - id-token: write permissions

2. **Sigstore Signing**
   - Cryptographic signatures on releases
   - Supply chain security

3. **Dependency Scanning**
   - CodeQL for code vulnerabilities
   - pip-audit for dependency issues
   - Dependabot for automated updates

4. **Principle of Least Privilege**
   - Minimal permissions per job
   - Read-only by default

### ✅ Code Quality

1. **Multi-stage Linting**
   - Ruff for style + formatting
   - mypy for type checking
   - Fail fast on quality issues

2. **Comprehensive Testing**
   - Unit tests across Python versions
   - Integration tests with services (Redis)
   - Coverage tracking and reporting

### ✅ Modern Tooling

1. **uv Package Manager**
   - Blazing fast Rust-based tool
   - Better than pip/poetry for CI
   - Built-in caching support

2. **Latest Action Versions**
   - `actions/checkout@v4`
   - `actions/setup-python@v5`
   - `astral-sh/setup-uv@v5`

3. **Service Containers**
   - Redis 7 Alpine (lightweight)
   - Health checks built-in

## Usage

### Running Tests Locally

```bash
# Install dependencies
uv sync

# Run linting
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Run type checking
uv run mypy src/

# Run tests
uv run pytest -v --cov=src/pyergon
```

### Manual Deployment

1. Go to Actions → Publish to PyPI
2. Click "Run workflow"
3. Select environment (testpypi or pypi)
4. Click "Run workflow"

### Creating a Release

```bash
# Tag the release
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0

# Create GitHub release
gh release create v0.1.0 --generate-notes
```

The publish workflow will automatically trigger and deploy to PyPI.

## Monitoring

- **Test Results**: Check Actions tab for test pass/fail
- **Coverage**: View Codecov dashboard (link in repo)
- **Security**: Check Security tab for vulnerabilities
- **Dependencies**: Check Dependabot PRs

## Troubleshooting

### Workflow Fails on Dependency Installation

- Check `uv.lock` is committed
- Ensure Python version matches (3.11+)
- Review uv cache key in workflow

### PyPI Publish Fails

- Verify trusted publishing is configured in PyPI
- Check version number isn't already published
- Review workflow permissions (id-token: write)

### Tests Timeout

- Check test timeout settings (default: 30s)
- Review async test fixtures
- Check Redis service health

## Contributing

When modifying workflows:

1. Test changes on a feature branch first
2. Use `act` for local workflow testing
3. Review workflow runs before merging
4. Update this README with changes

## References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [uv Documentation](https://docs.astral.sh/uv/)
- [Trusted Publishing Guide](https://docs.pypi.org/trusted-publishers/)
- [CodeQL Documentation](https://codeql.github.com/docs/)
