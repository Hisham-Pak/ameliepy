# Amelie Python Driver (`ameliepy`)

A Python database driver for [amelie](https://amelielabs.io/docs/).  
This project provides DB-API 2.0–style access to amelie, enabling seamless integration with Python applications.

📖 Documentation: [ameliepy.readthedocs.io](https://ameliepy.readthedocs.io/)
🐍 PyPI: [pypi.org/project/ameliepy/](https://pypi.org/project/ameliepy/)

---

## 🚀 Development Setup

Clone the repository and install dependencies using [uv](https://github.com/astral-sh/uv):

```bash
# Install dependencies
uv sync

# Editable install
uv pip install -e .
```

## ▶️ Managing Amelie

### Start AmelieDB
```bash
amelie start ./repo
```

### Stop AmelieDB
```bash
amelie stop ./repo
```

Read more at: https://amelielabs.io/docs/tutorial/get_started/

## ⚙️ Environment Variables

Before running tests, configure your environment:

```bash
export host="http://localhost:3485"
export schema="ameliepy_test"
```

## 🧪 Running Tests

Execute the test suite with:

```bash
uv run python -m pytest -sq
```

## 🤝 Contributing

Contributions are welcome! Please open issues or pull requests to help improve the driver.
