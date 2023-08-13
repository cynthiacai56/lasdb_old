'''
import laspy
import numpy as np

def write_las_file(points, output_file_path):
    # Check if the numpy array has the required fields
    required_fields = ['X', 'Y', 'Z', 'Classification']
    if not all(field in points.dtype.names for field in required_fields):
        raise ValueError("Input numpy array must contain fields: X, Y, Z, and Classification.")

    # Create a new LAS file
    outfile = laspy.file.File(output_file_path, mode='w', header=laspy.header.Header())

    # Set LAS header fields
    outfile.header.scale = [0.01, 0.01, 0.01]  # Adjust the scale as needed
    outfile.header.offset = [0, 0, 0]          # Adjust the offset as needed
    outfile.header.data_format_id = 3          # 3 for point format 3 (X, Y, Z, intensity, and classification)
    outfile.header.point_format_id = 3         # 3 for point format 3 (X, Y, Z, intensity, and classification)

    # Set the values of the attributes
    outfile.X = points['X']
    outfile.Y = points['Y']
    outfile.Z = points['Z']
    #outfile.intensity = np.zeros_like(x)       # Set intensity as zero (change as needed)
    outfile.classification = points['Classification']

    # Close the LAS file
    outfile.close()

# Example usage:
# Assuming you have a numpy array called 'points_array' with fields X, Y, Z, and Classification.
# write_las_file(points_array, 'output_file.las')
'''