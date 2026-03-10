# Deep dive into the original temp file
with open(r"c:\Users\Emilio\SISRI\temp\Brands_Brands_cleaned.txt", 'rb') as f:
    content = f.read()
    
print(f"File size: {len(content)} bytes")
print(f"Raw first 200 bytes: {content[:200]}")
print(f"Hex dump of first 100 bytes: {content[:100].hex()}")

# Look for patterns
print("\nLooking for tab patterns:")
for i, byte in enumerate(content[:100]):
    if byte == 9:  # Tab
        print(f"Tab at position {i}")
        
print("\nLooking for \\r\\n patterns:")
for i in range(len(content) - 1):
    if content[i] == 13 and content[i+1] == 10:  # \r\n
        print(f"\\r\\n at positions {i}-{i+1}")
