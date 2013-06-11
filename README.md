PyTools
=======

Various Python Utilities

storage-archive - Amazon Glacier Storage Archiver
  - Archives files / directories to Amazon Glacier
  - Breaks up the archive into multiple .tar.gz files to reduce AWS download costs of restores.
  - Encrypts archive files before sending to Glacier
  - Create / Delete Glacier Vaults
  - Track all archived data using SQLite database

storage-report - Report on file storage utilization
  - Parse through a directory structure storing details on all files.
  - Store details in SQLite database.
  - Create storage reports from parsed data.
