import os
import re

if __name__ == "__main__":
    for file in os.listdir("log"):
        if not file.endswith(".log"):
            continue

        path = os.path.join("log", file)

        with open(path, "r") as f:
            lines = f.readlines()

        new_lines = []

        for line in lines:
            if "__CALL__" in line:
                continue

            new_lines.append(re.sub(r"\|.*\[0m?", "|", line))

        with open(path, "w") as f:
            f.writelines(new_lines)
