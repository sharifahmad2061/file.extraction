import os
import os.path
import hashlib
import uuid
import datetime
import time
import threading
import queue
import shutil
import patoolib
# import mysql.connector
import sys
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
# from mysql.connector import Error

#############################################
# Config
#############################################

# Paths
# path_to_files       = "D:/Users/Tobias/VisualStudio Repro/CleanMyFile/Files"
path_to_files = "E:/freelance-projects/data.wrangling.company.job/Files"
path_for_inbox = path_to_files + "/1Inbox"
path_for_extracts = path_to_files + "/1Inbox/1Extracts_xz37a"
path_for_ready = path_to_files + "/2Ready"
path_for_processing = path_to_files + "/3Processing"
path_for_errors = path_to_files + "/4Error"
path_for_processed = path_to_files + "/5Processed"
path_for_originals = path_to_files + "/ZOriginals"
path_for_temp = path_to_files + "/ZTemp"

# MySQL
mysql_host = "localhost"
mysql_user = "root"
mysql_pwd = ""
mysql_db = "cleanmyfile"


#############################################
# File Observer - Wachtes for new files
#############################################
class OnMyWatch:
    # Set the directory on watch
    watchDirectory = path_for_inbox

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(
            event_handler, self.watchDirectory, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Observer Stopped")

        self.observer.join()


#############################################
# Event Handler - Process file when detected
#############################################
class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            #############################################
            # Event is created, processing starts now
            #############################################
            print(f'-----------------------------------------------')
            print("New file was detetcted - % s" % event.src_path)

            #############################################
            # Specify path to file
            #############################################
            file_path = event.src_path
            print(f'File path:                      {file_path}')

            #############################################
            # Timestamp for when file processing START
            #############################################
            date_processing_start = datetime.datetime.now()
            print(f'Start processing file:          {date_processing_start}')

            #############################################
            # Generate UUIDv4 for file
            #############################################
            uuid_str_file = uuid.uuid4().urn
            file_uuid = uuid_str_file[9:]
            print(f'File UUID:                      {file_uuid}')

            #############################################
            # Connect to database
            #############################################
            # try:
            #     connection = mysql.connector.connect(host=mysql_host,
            #                                         database=mysql_db,
            #                                         user=mysql_user,
            #                                         password=mysql_pwd)
            #     if connection.is_connected():
            #         db_Info = connection.get_server_info()
            #         print("Connected to MySQL Server version ", db_Info)
            #         cursor = connection.cursor()
            #         cursor.execute("select database();")
            #         record = cursor.fetchone()
            #         print("Connected to database: ", record)

            # except Error as e:
            #     print("Error while connecting to MySQL", e)
            #     sys.exit()  # Send Telegram notification before exit !!!!!!!!!!!!!

            #############################################
            # File stats
            #############################################
            file_stats = os.stat(file_path)
            file_extension_str = os.path.splitext(file_path)[1]
            # Important otherwise upper case extensions would not be handeled correctly
            file_extension_original = file_extension_str.lower()
            file_name_original = os.path.basename(file_path)
            file_size_kb_original = file_stats.st_size / 1000
            meta_accessed_original = datetime.datetime.utcfromtimestamp(
                file_stats.st_atime).strftime('%Y-%m-%dT%H:%M:%SZ')
            meta_created_original = datetime.datetime.utcfromtimestamp(
                file_stats.st_ctime).strftime('%Y-%m-%dT%H:%M:%SZ')
            meta_modified_original = datetime.datetime.utcfromtimestamp(
                file_stats.st_mtime).strftime('%Y-%m-%dT%H:%M:%SZ')

            print(f'File name:                      {file_name_original}')
            print(f'File extension:                 {file_extension_original}')
            print(
                f'File size original in KB:       {file_stats.st_size / 1024}')
            print(f'Date last access:               {file_stats.st_atime}')
            print(f'Date creation:                  {file_stats.st_ctime}')
            print(f'Date modification:              {file_stats.st_mtime}')

            #############################################
            # Hashing file with MD5 for deduplication
            #############################################
            hash_blocksize = 65536

            try:
                hasher_md5 = hashlib.md5()
                with open(file_path, 'rb') as afile:
                    buf = afile.read(hash_blocksize)
                    while len(buf) > 0:
                        hasher_md5.update(buf)
                        buf = afile.read(hash_blocksize)
                md5_original = hasher_md5.hexdigest()

                print(
                    f'MD5 hash original:              {hasher_md5.hexdigest()}')

                #############################################
                # Duplicate check - Was file seen before?
                #############################################
                # cursor = connection.cursor()
                # query = "SELECT COUNT(md5_original) FROM file_master WHERE md5_original = %s"
                # cursor.execute(query, [md5_original,])

                # COUNT query always return value, if no matching data
                # it simply return integer value 0, so we can safely take data
                # from result[0]
                # result_check = cursor.fetchone()
                # cursor.close()

                # if not result_check[0]:
                #     print("This is a new file that was never seen before.")
                #     result = 1;

                    #############################################
                    # Insert file details into database
                    #############################################
                    # try:
                    #     cursor = connection.cursor()
                    #     sql_file_details = """INSERT INTO file_master (file_uuid, timestamp_processing_start, file_path_original, file_name_original, file_extension_original, file_size_kb_original, md5_original, meta_created_original, meta_accessed_original, meta_modified_original)
                    #             VALUES
                    #             (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

                    #     file_details_tuple = (file_uuid, date_processing_start, file_path, file_name_original, file_extension_original, file_size_kb_original, md5_original, meta_created_original, meta_accessed_original, meta_modified_original)
                    #     cursor.execute(sql_file_details, file_details_tuple)
                    #     connection.commit()
                    #     cursor.close()
                    #     print("File details inserted successfully into database")

                    # except mysql.connector.Error as error:
                    #     print("Failed to insert file details into MySQL table {}".format(error))

                #############################################
                # Extract if file is archive
                #############################################
                archive_extensions_permitted = [".zip", ".7z", ".cb7", ".ace", ".cba", ".adf", ".alz", ".ape", ".a", ".arc", ".arj", ".bz2",
                                                ".cab", ".Z", ".cpio", ".deb", ".dms", ".flac", ".gz", ".iso", ".lrz", ".lha", ".lzh", ".lz",
                                                ".lzma", ".lzo", ".rpm", ".rar", ".cbr", ".rz", ".shn", ".tar", ".cbt", ".xz", ".zip", ".jar",
                                                ".cbz", ".zoo", ".zpaq"]

                if file_extension_original in archive_extensions_permitted:

                    # Path config
                    extraction_path = path_for_extracts + '/' + file_uuid

                    # Timestamp extraction start
                    date_extraction_start = datetime.datetime.now()

                    # Extract archive
                    try:
                            patoolib.extract_archive(
                                file_path, outdir=extraction_path)
                            print("Archiv successfully extracted")
                    except:
                            print(f'Error extracting file')

                            # Move failed file to folder 'Errors'
                            os.rename(file_path, path_for_errors +
                                      '/' + file_uuid + file_extension_original)

                            # Record error in database
                            # cursor = connection.cursor()
                            # sql_error_extraction = """INSERT INTO file_errors (fk_file_uuid, timestamp_error, error_code, error_name)
                            #         VALUES
                            #         (%s, %s, %s, %s) """

                            # extraction_details_tuple = (file_uuid, timestamp_error_extraction, "FEX001", "File extraction failed")
                            # cursor.execute(sql_error_extraction, extraction_details_tuple)
                            # connection.commit()
                            # cursor.close()

                    # Timestamp extraction end
                    date_extraction_end = datetime.datetime.now()

                    # Write extraction stats to database
                    # cursor = connection.cursor()
                    # sql_extraction_query = """Update file_master set timestamp_extraction_start = %s, timestamp_extraction_end = %s where file_uuid = %s"""

                    # extraction_input = (date_extraction_start, date_extraction_end, file_uuid)

                    # cursor.execute(sql_extraction_query, extraction_input)
                    # connection.commit()
                    # cursor.close()
                    print("Extraction stats sucessfully written to database ")

                    # Copy archive file to folder 'Originals'
                    shutil.copy2(file_path, path_for_originals + '/archives/' +
                                 file_uuid + file_extension_original, follow_symlinks=True)

                    # Timestamp when error occured
                    timestamp_error_extraction = datetime.datetime.now()

                #############################################
                # Convert Excel files to CSV
                #############################################
                excel_extensions_permitted = [".xls", ".xlsx"]

                if file_extension_original in excel_extensions_permitted:

                        # Print start conversion
                        print("File conversion started")

                        # Record timestamp conversion begin
                        date_conversion_start = datetime.datetime.now()

                        # Read and store content of an excel file
                        try:
                            read_file = pd.read_excel(file_path)

                            # Write the dataframe object into csv file
                            read_file.to_csv(path_for_ready + '/' + file_uuid + ".csv",
                                            index=None,
                                            header=True,
                                            chunksize=1000)
                        except:
                            print(f'Error converting file')
                            pass  # Only for testing!! Write error handler with db connect !!!!!!!!!!!!!!!!!!!!!!!!!

                        # Record timestamp conversion end
                        date_conversion_end = datetime.datetime.now()

                        # Print success in cmd
                        print("Excel successfully converted to CSV")

                        # Path after conversion
                        path_after_conversion = path_for_ready + '/' + file_uuid + ".csv"
                        print(
                            f'New path after conversion:      {path_after_conversion}')

                        # Obtain new file size
                        size_after_conversion = os.path.getsize(
                            path_after_conversion) / 1000
                        print(
                            f'New file size after conversion: {size_after_conversion}')

                        # Hash new file with MD5
                        hash_blocksize_converted = 65536

                        try:
                            hasher_md5_converted = hashlib.md5()
                            with open(path_after_conversion, 'rb') as afile:
                                buf = afile.read(hash_blocksize)
                                while len(buf) > 0:
                                    hasher_md5_converted.update(buf)
                                    buf = afile.read(hash_blocksize_converted)
                                md5_converted = hasher_md5_converted.hexdigest()
                                print(
                                    f'MD5 hash after conversion:      {md5_converted}')

                        except:
                            print(f'Error hashing file')
                            pass  # Only for testing!! Write error handler with db connect !!!!!!!!!!!!!!!!!!!!!!!!!

                        # Write conversion stats to database
                        # cursor = connection.cursor()
                        # sql_conversion_query = """Update file_master set md5_converted = %s, file_size_kb_converted = %s, timestamp_conversion_start = %s, timestamp_conversion_end = %s where file_uuid = %s"""

                        # conversion_input = (md5_converted, size_after_conversion, date_conversion_start, date_conversion_end, file_uuid)

                        # cursor.execute(sql_conversion_query, conversion_input)
                        # connection.commit()
                        # cursor.close()
                        print("Conversion stats sucessfully written to database ")

                        # Move excel file from 'Inbox' to folder 'Originals'
                        os.rename(file_path, path_for_originals +
                                  '/excel/' + file_uuid + file_extension_original)

                #############################################
                # Convert Text files to CSV
                #############################################
                txt_extensions_permitted = [".txt"]

                if file_extension_original in txt_extensions_permitted:

                    # Print start conversion
                    print("File conversion started")
            
                    # Record timestamp conversion begin
                    date_conversion_txt_start = datetime.datetime.now()
            
                    # Reading text file
                    text_file = pd.read_csv(file_path)      # At the moment only works if txt is comma delimited !!!!!!!

                    # Storing this dataframe in a csv file 
                    try:
                        text_file.to_csv(path_for_ready + '/' + file_uuid + ".csv",  
                                        index = None,
                                        chunksize=1000)
            
                    except:
                        print(f'Error converting file')
                        pass  #Only for testing!! Write error handler with db connect !!!!!!!!!!!!!!!!!!!!!!!!!

                    # Record timestamp conversion end
                    date_conversion_txt_end = datetime.datetime.now()

                    # Print success in cmd
                    print("Text file successfully converted to CSV")

                    # Path after conversion
                    path_after_conversion_txt = path_for_ready + '/' + file_uuid + ".csv"
                    print(f'New path after conversion:      {path_after_conversion_txt}')

                    # Obtain new file size
                    size_after_conversion_txt = os.path.getsize(path_after_conversion_txt) / 1000
                    print(f'New file size after conversion: {size_after_conversion_txt}')

                    # Hash new file with MD5
                    hash_blocksize_converted_txt = 65536

                    try:
                            hasher_md5_converted_txt = hashlib.md5()
                            with open(path_after_conversion_txt, 'rb') as afile:
                                buf = afile.read(hash_blocksize)
                                while len(buf) > 0:
                                    hasher_md5_converted_txt.update(buf)
                                    buf = afile.read(hash_blocksize_converted_txt)
                            md5_converted_txt = hasher_md5_converted_txt.hexdigest()
                            print(f'MD5 hash after conversion:      {md5_converted_txt}')

                    except:
                        print(f'Error hashing file')
                        pass  #Only for testing!! Write error handler with db connect !!!!!!!!!!!!!!!!!!!!!!!!!

                    # Move to original folder
                    os.rename(file_path, path_for_originals + '/txt/' + file_uuid + file_extension_original)

                    # Write conversion stats to database
                    # cursor = connection.cursor()
                    # sql_conversion_txt_query = """Update file_master set md5_converted = %s, file_size_kb_converted = %s, timestamp_conversion_start = %s, timestamp_conversion_end = %s where file_uuid = %s"""

                    # conversion_txt_input = (md5_converted_txt, size_after_conversion_txt, date_conversion_txt_start, date_conversion_txt_end, file_uuid)

                    # cursor.execute(sql_conversion_txt_query, conversion_txt_input)
                    # connection.commit()
                    # cursor.close()
                    print("Conversion stats sucessfully written to database ")

       
                #############################################
                # Move csv files to appropriate folders
                #############################################
                csv_extensions_permitted = [".csv"]   # Add to unexpected file array exemption !!!!

                if file_extension_original in csv_extensions_permitted:
        
                    # Copy archive file to folder 'Originals' 
                    shutil.copy2(file_path, path_for_originals + '/csv/' + file_uuid + ".csv", follow_symlinks=True)

                    # Move csv file from 'Inbox' to folder 'Ready' 
                    os.rename(file_path, path_for_ready + '/' + file_uuid + ".csv")


                #############################################
                # Handler for unexpected files
                #############################################
                # Combines all permitted extensions that were specified before
                all_extensions_permitted = csv_extensions_permitted + txt_extensions_permitted + excel_extensions_permitted + archive_extensions_permitted
        
                if file_extension_original not in all_extensions_permitted:
            
                    # Copy to 'Originals"
                    shutil.copy2(file_path, path_for_originals + '/other/' + file_uuid + file_extension_original, follow_symlinks=True)

                    # Move to 'Error' folder
                    os.rename(file_path, path_for_errors + '/' + file_uuid + file_extension_original)

                    # Timestamp when error occured
                    timestamp_unexpected_file = datetime.datetime.now()

                    # Print unexpected file message
                    print("This is a {file_extension_original} file which is not supported.  ")
                    print("An error was created and file can be reviewed and processed manually.  ")

                    # Record error in database
                    # cursor = connection.cursor()
                    # sql_unexpected_file = """INSERT INTO file_errors (fk_file_uuid, timestamp_error, error_code, error_name) 
                    #         VALUES 
                    #     (   %s, %s, %s, %s) """

                    # unexpected_file_tuple = (file_uuid, timestamp_unexpected_file, "UNE001", "Unexpected file")
                    # cursor.execute(sql_unexpected_file, unexpected_file_tuple)
                    # connection.commit()
                    # cursor.close()

                    #############################################
                    # Timestamp for when file processing ENDED
                    #############################################           

                    # Generate timestamp
                    date_processing_end = datetime.datetime.now()

                    # Write to database
                    # cursor = connection.cursor()
                    # sql_endprocessing_query = """Update file_master set timestamp_processing_end = %s where file_uuid = %s"""

                    # endprocessing_input = (date_processing_end, file_uuid)

                    # cursor.execute(sql_endprocessing_query, endprocessing_input)
                    # connection.commit()
                    # cursor.close()

                    # Print end of processing
                    print("File processing has finished. ")
            
                #############################################
                # Handler if file is a duplicate
                #############################################
                else:
                    print("This is a DUPLICATE of an already processed file!")
                    os.remove(file_path)
                    print("File was deleted")
                    result = 0
            
            #############################################
            # Error Handler - Original Hashing Failed
            #############################################
            except:
                print(f'Error hashing original file - HAS001')

                # Timestamp extraction start
                date_error_hashing_original = datetime.datetime.now()

                # Move failed file to folder 'Errors' 
                os.rename(file_path, path_for_errors + '/' + file_uuid + file_extension_original)

                # Record error in database
                # cursor = connection.cursor()
                # sql_error_hashing_original = """INSERT INTO file_errors (fk_file_uuid, timestamp_error, error_code, error_name) 
                #                     VALUES 
                #                     (%s, %s, %s, %s) """

                # hashing_original_tuple = (file_uuid, date_error_hashing_original, "HAS001", "Hashing of original file failed")
                # cursor.execute(sql_error_hashing_original, hashing_original_tuple)
                # connection.commit()
                # cursor.close()

if __name__ == '__main__': 
    watch = OnMyWatch() 
    watch.run() 
