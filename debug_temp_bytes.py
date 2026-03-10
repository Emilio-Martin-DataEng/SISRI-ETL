# Check the temp file content and encoding
with open(r"c:\Users\Emilio\SISRI\temp\Brands_Brands_cleaned.txt", 'rb') as f:
    first_line_bytes = f.readline()
    print(f"Raw bytes: {first_line_bytes}")
    print(f"Decoded: {first_line_bytes.decode('utf-8')}")
    
    # Check for tab characters
    tabs_count = first_line_bytes.count(b'\t')
    print(f"Number of tabs: {tabs_count}")
    
    # Split by tabs
    columns = first_line_bytes.decode('utf-8').strip().split('\t')
    print(f"Number of columns: {len(columns)}")
    for i, col in enumerate(columns):
        print(f"  Column {i+1}: '{col}'")
