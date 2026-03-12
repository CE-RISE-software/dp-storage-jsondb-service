# CE-RISE Digital Passport JSON Database Storage Service
This site contains the technical documentation for the CE-RISE ...

Use the navigation sidebar to access architecture, API, configuration, deployment,
adapter contract, and operations documentation.

---

## First 5 Minutes

This quickstart verifies that the service is reachable and can process one validation request.

### 1. Start the service

Run your deployed container (or local instance) with the required environment variables configured.

### 2. Check health

```bash
curl http://localhost:8080/admin/health
```

Expected response:

```json
{"status":"ok"}
```

### 3. Check loaded models

```bash
curl http://localhost:8080/models
```

Pick one `model` and `version` from the response.

### 4. Validate a payload

```bash
curl -X POST "http://localhost:8080/models/<model>/versions/<version>:validate" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {
      "id": "example-001",
      "name": "example record"
    }
  }'
```

Expected response shape:

```json
{
  "passed": true,
  "results": []
}
```

If your auth mode is `none`, you can omit the `Authorization` header.


---

Funded by the European Union under Grant Agreement No. 101092281 — CE-RISE.  
Views and opinions expressed are those of the author(s) only and do not necessarily reflect those of the European Union or the granting authority (HADEA).
Neither the European Union nor the granting authority can be held responsible for them.

<a href="https://ce-rise.eu/" target="_blank" rel="noopener noreferrer">
  <img src="images/CE-RISE_logo.png" alt="CE-RISE logo" width="200"/>
</a>

© 2026 CE-RISE consortium.  
Licensed under the [European Union Public Licence v1.2 (EUPL-1.2)](https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12).  
Attribution: CE-RISE project (Grant Agreement No. 101092281) and the individual authors/partners as indicated.

<a href="https://www.nilu.com" target="_blank" rel="noopener noreferrer">
  <img src="https://nilu.no/wp-content/uploads/2023/12/nilu-logo-seagreen-rgb-300px.png" alt="NILU logo" height="20"/>
</a>

Developed by NILU (Riccardo Boero — ribo@nilu.no) within the CE-RISE project.
