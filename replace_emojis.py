import glob
emojis = {
    "[ERRO]": "[ERRO]",
    "[SEND]": "[SEND]",
    "[FILE]": "[FILE]",
    "[STEP]": "[STEP]",
    "[DONE]": "[DONE]",
    "-->": "-->",
    "-->": "-->",
    "-->": "-->",
    "-->": "-->",
    "[WARN]": "[WARN]",
    "[INFO]": "[INFO]",
    "[INFO]": "[INFO]",
    "->": "->",
    "-": "-" 
}

files = glob.glob("**/*.py", recursive=True)
for f in files:
    with open(f, 'r', encoding='utf-8') as ifile:
        content = ifile.read()
    for e, r in emojis.items():
        content = content.replace(e, r)
    with open(f, 'w', encoding='utf-8') as ofile:
        ofile.write(content)
print("Replaced emojis in", len(files), "files")
