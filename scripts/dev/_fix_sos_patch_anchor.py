from pathlib import Path

path = Path("scripts/dev/_apply_sos_station_identity_fix.py")
text = path.read_text(encoding="utf-8")
old = '''def replace_once(text: str, old: str, new: str, label: str) -> str:\n    count = text.count(old)\n    if count != 1:\n        raise RuntimeError(f"Patch anchor {label!r} matched {count} times")\n    return text.replace(old, new, 1)\n'''
new = '''def replace_once(text: str, old: str, new: str, label: str) -> str:\n    count = text.count(old)\n    if label == "catalogue station label map filter" and count == 2:\n        first = text.index(old)\n        second = text.index(old, first + len(old))\n        return f"{text[:second]}{new}{text[second + len(old):]}"\n    if count != 1:\n        raise RuntimeError(f"Patch anchor {label!r} matched {count} times")\n    return text.replace(old, new, 1)\n'''
if text.count(old) != 1:
    raise RuntimeError("Unable to locate guarded replace_once helper")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
print("Adjusted the catalogue geometry-map patch anchor")
