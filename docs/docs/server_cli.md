# Server CLI

Indexify Control Plane is two Different Servers, Ingestion Server and Coordinator Server, packaged in one binary. We will show you how you can run them together in `Dev` mode, or run these servers separately.

### Download the CLI
```bash
curl https://www.tensorlake.ai | sh
```

### Start in Dev Mode
Start the Ingestion Server and Coordinator in the same process with the default configuration.

```bash
indexify server -d
```

### Generate the Sample Config

Generate the sample config in the current directory
```bash
indexify init-config
```

### Start in Dev Mode with Custom Config
```bash
indexify server -d -c /path/to/config.yaml
```


### Start the Ingestion Server
```bash
indexify server -c /path/to/config.yaml
```

### Start the Coordinator Server
```bash
indexify coordinator -c /path/to/config.yaml
```