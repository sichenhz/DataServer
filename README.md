### How to run DataServer

1. Configure the Tweets.txt's absolute path to a parameter of DataStreamGenerator in Run Configurations.
``` bash
String txtFile = args[0]; // "/Users/Jason/Github/DataServer/Tweets.txt"
```

2. You should run DataServer and DataStreamGenerator first as they are both servers.

3. Then you should run Worker as it is a slave of DataServer.

4. Finally you can run Client.
