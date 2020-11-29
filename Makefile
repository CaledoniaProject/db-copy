run: build
	java -jar $(PWD)/target/db-copy-1.0.jar data/mysql.ini

build:
	mvn clean package -Dmaven.test.skip=true
