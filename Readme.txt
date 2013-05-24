mvn --version
mvn archetype:generate -DgroupId=com.mvdb.platform -DartifactId=mvdb -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false
cd mvdb
mvn package
java -cp target/mvdb-1.0-SNAPSHOT.jar com.mvdb.platform.App
