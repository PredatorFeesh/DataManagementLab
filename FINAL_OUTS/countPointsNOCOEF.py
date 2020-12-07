import sys

data = []
with open(sys.argv[1], "r") as f:
    for line in f:
        data.append([int(i.strip("\n")) for i in line.split(",")[1:]])

S = 0
for item in data:
    S += sum(item)

print(S)
