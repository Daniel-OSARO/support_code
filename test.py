nvrname1 = "192.168.111.12:8010"
nvrname2 = "192.168.111.16:8010"

# Extract cell number for nvrname1
cell_number1 = int(nvrname1.split('.')[-1].split(':')[0])%10
print(f"Cell number from nvrname1: {cell_number1}")

# Extract cell number for nvrname2
cell_number2 = int(nvrname2.split('.')[-1].split(':')[0])%10
print(f"Cell number from nvrname2: {cell_number2}")
