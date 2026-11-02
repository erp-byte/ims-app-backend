"""Regenerates FDK_FILES.md — the inventory of every fdk path and the functions in it.

Run from the backend root:  python scripts/fdk_files_md.py
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "FDK_FILES.md"

FDK_DIRS = [
    "services/fdk_auth_service",
    "services/fdk_detector_service",
    "services/fdk_fyers_service",
    "services/fdk_mcp_service",
    "services/fdk_penny_service",
    "services/fdk_sector_service",
    "services/fdk_sheets_service",
    "services/fdk_summary_service",
    "services/fdk_supervisor_service",
    "services/fdk_telegram_service",
    "fdk_tests",
]
FDK_FILES = [
    "fdk_main.py",
    "fdk_requirements.txt",
    "fdk_requirements_nodeps.txt",
    "fdk_env.example",
    "FDK_FILES.md",
    "scripts/fdk_files_md.py",
    "shared/fdk_config_loader.py",
    "shared/fdk_constants.py",
    "shared/fdk_exceptions.py",
    "shared/fdk_logger.py",
    "shared/fdk_models.py",
    "config/fdk_sectors.json",
    "config/fdk_symbols.json",
    "docs/fdk_CLAUDE.md",
]
# Existing backend files that carry fdk-marked lines (grep for "fdk").
TOUCH_POINTS = ["main.py", "requirements.txt", "Dockerfile"]
SKIP = {"__pycache__", ".pytest_cache"}


def functions(py: Path) -> list[str]:
    """Dotted names of every class/function in the file, at any nesting depth."""
    tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))
    names: list[str] = []

    def walk(body, prefix=""):
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = f"{prefix}{node.name}"
                names.append(name)
                walk(node.body, name + ".")
            elif isinstance(node, (ast.If, ast.For, ast.While, ast.With, ast.Try)):
                walk(getattr(node, "body", []), prefix)
                for extra in ("orelse", "finalbody"):
                    walk(getattr(node, extra, []), prefix)
                for handler in getattr(node, "handlers", []):
                    walk(handler.body, prefix)

    walk(tree.body)
    return names


def all_paths() -> list[Path]:
    paths: list[Path] = []
    for d in FDK_DIRS:
        base = ROOT / d
        if not base.exists():
            continue
        for p in sorted(base.rglob("*")):
            if p.is_file() and not (SKIP & set(p.relative_to(ROOT).parts)):
                paths.append(p)
    for f in FDK_FILES:
        p = ROOT / f
        if (p.exists() or p == OUT) and p not in paths:
            paths.append(p)
    return sorted(set(paths), key=lambda p: p.relative_to(ROOT).as_posix())


def render() -> str:
    lines = [
        "# FDK_FILES.md",
        "",
        "Every file that belongs to fdk (the Fyers Volume Spike Detector merged from fyersdk), "
        "with the classes/functions each one defines. Regenerate with `python scripts/fdk_files_md.py`.",
        "",
        "## fdk files",
        "",
    ]
    for p in all_paths():
        rel = p.relative_to(ROOT).as_posix()
        lines.append(f"### {rel}")
        if p.suffix == ".py":
            names = functions(p)
            lines.extend(f"- {n}" for n in names) if names else lines.append("- (none)")
        else:
            lines.append("- (not python)")
        lines.append("")
    lines += [
        "## Touch points in existing files (lines marked `fdk`)",
        "",
    ]
    lines.extend(f"- {t}" for t in TOUCH_POINTS)
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    OUT.write_text(render(), encoding="utf-8", newline="\n")
    print(f"wrote {OUT.relative_to(ROOT)}")
