#database

A. get code:
    1. git clone https://github.com/CK0722/database.git
    2. mvn install
    3. cd ./target/classes/

B. run code:
    1.split the dataset into different files:
        java -Djava.ext.dirs=mvnRepositoryPath  cn/sky/database/controller/SplitDataController absoluteDataSetFilePath absoluteRepositoryPath rows
    2.create index based on the specified column of the dataset:
        java -Djava.ext.dirs=mvnRepositoryPath  cn/sky/database/controller/IndexController absoluteRepositoryPath absoluteIndexPath column capcity
    3.query data without index:
        java -Djava.ext.dirs=mvnRepositoryPath  cn/sky/database/controller/QueryWithoutIndexController  absoluteRepositoryPath column value threadSize
