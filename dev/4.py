a = "table"
b = a.split(".")
for i in range(a.count("."), 2):
    b.insert(0, None)

print(b)
a1, a2, a3 = b
print(a1, a2, a3)
