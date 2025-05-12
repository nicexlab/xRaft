# Installation

```bash
docker build -t xraft-tlc-verifier .
docker run -it xraft-tlc-verifier bash

# in docker bash:
tlc -config xraft_spec/Fast.cfg xraft/Fast.tla
```
