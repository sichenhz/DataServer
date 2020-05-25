### How to run DataServer

1. cd to src directory
``` bash
    cd /Users/Jason/Github/DataServer/src
```

2. Login to the instance by SSH
``` bash
    ssh -i kit318tut.pem ubuntu@115.146.87.21
```

3. Run DataStreamGenerator
``` bash
    java -jar /home/ubuntu/upload/DataStreamGenerator.jar
```

4. Run DataServer
``` bash
    java -jar /home/ubuntu/upload/DataServer.jar
```

5. back to local cmd and cd to bin directory and run Client.class for testing the server
``` bash
    cd ../bin
    java Client
```
