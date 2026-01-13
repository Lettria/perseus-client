import os
from perseus_client.client import PerseusClient


def main():
    file_path = "assets/example.txt"

    with PerseusClient() as client:
        print(f"Uploading file: {file_path}")
        uploaded_file = client.file.upload_file(file_path)
        print(
            f"Uploaded File ID: {uploaded_file.id}, Name: {uploaded_file.name}, Status: {uploaded_file.status}"
        )

        client.file.wait_for_file_upload(uploaded_file.id)

        print(f"Finding file with ID: {uploaded_file.id}")
        found_files = client.file.find_files(ids=[uploaded_file.id])
        if found_files:
            print(
                f"Found File ID: {found_files[0].id}, Name: {found_files[0].name}, Status: {found_files[0].status}"
            )
        else:
            print("File not found.")


if __name__ == "__main__":
    main()
