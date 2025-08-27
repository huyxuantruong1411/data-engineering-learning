import os

OUTPUT_FILE = "project_dump.txt"

def should_ignore_file(filename: str) -> bool:
    """Bỏ qua file không cần thiết"""
    if filename.startswith(".env"):  # Bỏ qua mọi file .env*
        return True
    if filename.endswith(".exe"):
        return True
    if filename == "__init__.py":
        return True
    return False

def should_ignore_dir(dirname: str) -> bool:
    """Bỏ qua thư mục không quan trọng"""
    if dirname.startswith(".env"):  # Bỏ qua mọi folder .env*
        return True
    if dirname in ("__pycache__", ".venv", "build", "dist"):
        return True
    return False

def build_tree(root_dir: str, prefix: str = "") -> str:
    """Tạo cây thư mục giống lệnh `tree` (lọc theo quy tắc ignore)."""
    entries = []
    with os.scandir(root_dir) as it:
        for entry in sorted(it, key=lambda e: e.name):
            if entry.is_dir() and not should_ignore_dir(entry.name):
                entries.append(entry)
            elif entry.is_file():
                if should_ignore_file(entry.name):
                    continue
                if entry.name.endswith(".py"):
                    entries.append(entry)

    lines = []
    for i, entry in enumerate(entries):
        connector = "└── " if i == len(entries) - 1 else "├── "
        if entry.is_dir():
            lines.append(prefix + connector + entry.name + "/")
            extension = "    " if i == len(entries) - 1 else "│   "
            lines.extend(build_tree(entry.path, prefix + extension).splitlines())
        else:
            lines.append(prefix + connector + entry.name)
    return "\n".join(lines)

def dump_project(root_dir: str, output_file: str):
    with open(output_file, "w", encoding="utf-8") as out:
        # In cấu trúc thư mục
        out.write("=== Project Tree ===\n")
        out.write(root_dir + "/\n")
        out.write(build_tree(root_dir))
        out.write("\n\n")

        # In nội dung code
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Lọc thư mục
            dirnames[:] = [d for d in dirnames if not should_ignore_dir(d)]

            rel_path = os.path.relpath(dirpath, root_dir)
            if rel_path == ".":
                rel_path = ""
            out.write(f"\n=== Folder: {rel_path or root_dir} ===\n")

            for filename in filenames:
                if should_ignore_file(filename):
                    continue
                if filename.endswith(".py"):
                    file_path = os.path.join(dirpath, filename)
                    out.write(f"\n--- File: {file_path} ---\n")
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            out.write(f.read())
                    except Exception as e:
                        out.write(f"[Lỗi đọc file: {e}]\n")

if __name__ == "__main__":
    current_dir = os.getcwd()
    dump_project(current_dir, OUTPUT_FILE)
    print(f"✅ Đã xuất toàn bộ code .py (đã lọc .env*, .exe, __pycache__, __init__.py...) + cây thư mục vào {OUTPUT_FILE}")