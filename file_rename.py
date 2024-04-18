import os


def replace_property_name(directory, old_property_name, new_property_name):
    # Iterate through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.pdf'):  # Process only PDF files
            original_path = os.path.join(directory, filename)

            # Replace old property name in the filename
            new_filename = filename.replace(f"{old_property_name}", f"{new_property_name}")
            new_path = os.path.join(directory, new_filename)

            # Rename the file
            os.rename(original_path, new_path)
            print(f"Renamed: {filename} -> {new_filename}")

    print("All files renamed successfully.")


# Specify the directory containing the files to be renamed
directory_to_rename = '/Users/davidmccandless/Downloads/ut'

# Specify the old and new property names
old_property_name = 'Cozy Cottage'
new_property_name = 'McCandless Consulting LLC'

# Call the function to replace property name in filenames
replace_property_name(directory_to_rename, old_property_name, new_property_name)
