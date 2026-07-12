#!/usr/bin/env python3
"""
从 scripts/bridge.py 自动生成 internal/manager/bridge_fallback.py

用法: python3 scripts/build_fallback.py
"""
import re
import sys

INPUT = "scripts/bridge.py"
OUTPUT = "internal/manager/bridge_fallback.py"

HEADER = '''#!/usr/bin/env python3
"""
MClaw Bridge (内置 fallback 版本)

此文件通过 go:embed 嵌入到 MClaw 二进制中，作为外部 scripts/bridge.py 的回退。
WS_URL 通过 %s 占位符由 Go fmt.Sprintf 在运行时注入。

⚠️ 自动生成文件 — 请勿手动编辑！
   修改源码后运行: python3 scripts/build_fallback.py
"""
'''

def main():
    with open(INPUT, "r") as f:
        code = f.read()

    # 1. 移除文件头 docstring
    code = re.sub(r'^#!/usr/bin/env python3\n"""[\s\S]*?"""\n', '', code, count=1)

    # 2. 移除单行注释（保留 inline 注释）
    lines = code.split('\n')
    result = []
    for line in lines:
        stripped = line.strip()
        # 保留 #go:embed 等特殊注释
        if stripped.startswith('#') and not stripped.startswith('# ─') and not stripped.startswith('#!'):
            continue
        result.append(line)
    code = '\n'.join(result)

    # 3. 移除多行 docstring（保留代码中的字符串）
    code = re.sub(r'    """[\s\S]*?"""', '', code)

    # 4. 压缩空行（最多保留 1 个空行）
    code = re.sub(r'\n{3,}', '\n\n', code)

    # 5. 替换 __WS_URL_B64__ 占位符为 Go %s
    code = code.replace('"__WS_URL_B64__"', '"%s"')

    # 6. 替换 __WS_URL__ 为 Go %s (if present)
    code = code.replace('"__WS_URL__"', '"%s"')

    # 7. 修复 Go 格式化占位符（%%H:%%M:%%S → %H:%M:%S）
    code = code.replace("%%H:%%M:%%S", "%H:%M:%S")

    # 8. 确保文件末尾有换行
    if not code.endswith('\n'):
        code += '\n'

    with open(OUTPUT, "w") as f:
        f.write(HEADER)
        f.write(code)

    print(f"✅ 已生成 {OUTPUT}")
    print(f"   源文件: {INPUT}")
    print(f"   行数: {code.count(chr(10))}")

if __name__ == "__main__":
    main()
