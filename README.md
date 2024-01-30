# apicurio-registry-delete-all-rules
Reproducer for Registry issue 4226

## How to run

`mvn compile exec:java -Dexec.mainClass="com.example.wittmann.SendSchema"`

This will run the SendSchema main Java class.  It will upload two versions of the
same Avro schema to Registry.  It should complete properly without force exiting.

## What you need

You'll need to be running Apicurio Registry on localhost.  This can be done with docker
like this:

```
docker pull quay.io/apicurio/apicurio-registry-mem:latest-release
docker run -it -p 8080:8080 quay.io/apicurio/apicurio-registry-mem:latest-release
```
