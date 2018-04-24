#database

A. get code:
    1. git clone https://github.com/CK0722/database.git
    2. mvn install
    3. cd ./target/classes/

B. run code:
    1.split the dataset into different files:
        java -Djava.ext.dirs=mvnRepositoryPath/com/opencsv/opencsv/3.10  cn/sky/database/controller/SplitDataController absoluteDataSetFilePath absoluteRepositoryPath rows