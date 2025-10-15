from hdfs import InsecureClient

client = InsecureClient('http://localhost:9870', user='hadoop')
hdfs_dir = '/tmp/demo'
file_path = hdfs_dir + '/data.txt'

def create_file(content):
    try:
        client.makedirs(hdfs_dir)
        with client.write(file_path, encoding='utf-8', overwrite=True) as writer:
            writer.write(content)
        print(f"File created at {file_path}")
    except Exception as e:
        print("Error creating file:", e)

def read_file():
    try:
        with client.read(file_path, encoding='utf-8') as reader:
            content = reader.read()
            print("File content:\n", content)
    except Exception as e:
        print("Error reading file:", e)

def update_file(new_data):
    try:
        with client.read(file_path, encoding='utf-8') as reader:
            old_content = reader.read()
        updated_content = old_content + "\n" + new_data
        with client.write(file_path, encoding='utf-8', overwrite=True) as writer:
            writer.write(updated_content)
        print("File updated successfully")
    except Exception as e:
        print("Error updating file:", e)

def delete_file():
    try:
        client.delete(file_path)
        print(f"File deleted from {file_path}")
    except Exception as e:
        print("Error deleting file:", e)

if __name__ == "__main__":
    print("Connecting to HDFS and running CRUD operations\n")
    create_file("Hi HDFS ")
    print("\nReading file after creation:")
    read_file()
    print("\nUpdating file with new data...")
    update_file("Hello HDFS")
    print("\nReading file after update:")
    read_file()
    print("\nDeleting file...")
    delete_file()
    print("\nCRUD operations on HDFS completed successfully")
