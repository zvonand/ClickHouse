---
slug: /en/operations/external-authenticators/jwt
---
# JWT
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Existing and properly configured ClickHouse users can be authenticated via JWT.

Currently, JWT can only be used as an external authenticator for existing users, which are defined in `users.xml` or in local access control paths.  
The username will be extracted from the JWT after validating the token expiration and against the signature. Signature can be validated by:
- static public key
- static JWKS
- received from the JWKS servers

It is mandatory for a JWT tot indicate the name of the ClickHouse user under `"sub"` claim, otherwise it will not be accepted.

A JWT may additionally be verified by checking the JWT payload.
In this case, the occurrence of specified claims from the user settings in the JWT payload is checked.
See [Enabling JWT authentication in `users.xml`](#enabling-jwt-auth-in-users-xml)

To use JWT authentication, JWT validators must be configured in ClickHouse config.


## Enabling JWT validators in ClickHouse {#enabling-jwt-validators-in-clickhouse}

To enable JWT validators, add `token_validators` section in `config.xml`. This section may contain several JWT verifiers, minimum is 1.

### Verifying JWT signature using static key {$verifying-jwt-signature-using-static-key}

**Example**
```xml
<clickhouse>
    <!- ... -->
    <jwt_validators>
        <my_static_key_validator>
          <algo>HS256</algo>
          <static_key>my_static_secret</static_key>
        </my_static_key_validator>
    </jwt_validators>
</clickhouse>
```

#### Parameters:

- `algo` - Algorithm for validate signature. Supported:

  | HMAC  | RSA   | ECDSA  | PSS   | EdDSA   |
  |-------| ----- | ------ | ----- | ------- |
  | HS256 | RS256 | ES256  | PS256 | Ed25519 |
  | HS384 | RS384 | ES384  | PS384 | Ed448   |
  | HS512 | RS512 | ES512  | PS512 |         |
  |       |       | ES256K |       |         |
  Also support None.
- `static_key` - key for symmetric algorithms. Mandatory for `HS*` family algorithms.
- `static_key_in_base64` - indicates if the `static_key` key is base64-encoded. Optional, default: `False`.
- `public_key` - public key for asymmetric algorithms. Mandatory except for `HS*` family algorithms and `None`.
- `private_key` - private key for asymmetric algorithms. Optional.
- `public_key_password` - public key password. Optional.
- `private_key_password` - private key password. Optional.

### Verifying JWT signature using static JWKS {$verifying-jwt-signature-using-static-jwks}

:::note
Only RS* family algorithms are supported!
:::

**Example**
```xml
<clickhouse>
    <!- ... -->
    <jwt_validators>
        <my_static_jwks_validator>
          <static_jwks>{"keys": [{"kty": "RSA", "alg": "RS256", "kid": "mykid", "n": "_public_key_mod_", "e": "AQAB"}]}</static_jwks>
        </my_static_jwks_validator>
    </jwt_validators>
</clickhouse>
```

#### Parameters:
- `static_jwks` - content of JWKS in json
- `static_jwks_file` - path to file with JWKS

:::note
Only one of `static_jwks` or `static_jwks_file` keys must be present in one verifier
:::

### Verifying JWT signature using JWKS servers {$verifying-jwt-signature-using-static-jwks}

**Example**
```xml
<clickhouse>
    <!- ... -->
    <jwt_validators>
        <basic_auth_server>
          <uri>http://localhost:8000/.well-known/jwks.json</uri>
          <connection_timeout_ms>1000</connection_timeout_ms>
          <receive_timeout_ms>1000</receive_timeout_ms>
          <send_timeout_ms>1000</send_timeout_ms>
          <max_tries>3</max_tries>
          <retry_initial_backoff_ms>50</retry_initial_backoff_ms>
          <retry_max_backoff_ms>1000</retry_max_backoff_ms>
          <refresh_ms>300000</refresh_ms>
        </basic_auth_server>
    </jwt_validators>
</clickhouse>
```

#### Parameters:

- `uri` - JWKS endpoint. Mandatory.
- `refresh_ms` - Period for resend request for refreshing JWKS. Optional, default: 300000.

Timeouts in milliseconds on the socket used for communicating with the server (optional):
- `connection_timeout_ms` - Default: 1000.
- `receive_timeout_ms` - Default: 1000.
- `send_timeout_ms` - Default: 1000.

Retry parameters (optional):
- `max_tries` - The maximum number of attempts to make an authentication request. Default: 3.
- `retry_initial_backoff_ms` - The backoff initial interval on retry. Default: 50.
- `retry_max_backoff_ms` - The maximum backoff interval. Default: 1000.

### Verifying access tokens {$verifying-access-tokens}

Access tokens that are not JWT (and thus no data can be extracted from the token directly) need to be resolved by external providers.

**Example**
```xml
<clickhouse>
    <!- ... -->
    <access_token_processors>
        <my_access_token_processor>
          <provider>google</provider>
        </my_access_token_processor>
    </access_token_processors>
</clickhouse>
```

#### Parameters:

- `provider` - name of provider that will be used for token processing. Mandatory parameter. Possible options: `google`.


### Enabling JWT authentication in `users.xml` {#enabling-jwt-auth-in-users-xml}

In order to enable JWT authentication for the user, specify `jwt` section instead of `password` or other similar sections in the user definition.

Parameters:
- `claims` - An optional string containing a json object that should be contained in the token payload.

Example (goes into `users.xml`):
```xml
<clickhouse>
    <!- ... -->
    <my_user>
        <!- ... -->
        <jwt>
            <claims>{"resource_access":{"account": {"roles": ["view-profile"]}}}</claims>
        </jwt>
    </my_user>
</clickhouse>
```

Here, the JWT payload must contain `["view-profile"]` on path `resource_access.account.roles`, otherwise authentication will not succeed even with a valid JWT.

```
{
...
  "resource_access": {
    "account": {
      "roles": ["view-profile"]
    }
  },
...
}
```

:::note
JWT authentication cannot be used together with any other authentication method. The presence of any other sections like `password` alongside `jwt` will force ClickHouse to shut down.
:::

### Enabling JWT authentication using SQL {#enabling-jwt-auth-using-sql}

When [SQL-driven Access Control and Account Management](/docs/en/guides/sre/user-management/index.md#access-control) is enabled in ClickHouse, users identified by JWT authentication can also be created using SQL statements.

```sql
CREATE USER my_user IDENTIFIED WITH jwt CLAIMS '{"resource_access":{"account": {"roles": ["view-profile"]}}}'
```

Or without additional JWT payload checks:

```sql
CREATE USER my_user IDENTIFIED WITH jwt
```

## JWT authentication examples {#jwt-authentication-examples}

#### Console client

```
clickhouse-client -jwt <token>
```

#### HTTP requests

```
curl 'http://localhost:8080/?' \
 -H 'Authorization: Bearer <TOKEN>' \
 -H 'Content type: text/plain;charset=UTF-8' \
 --data-raw 'SELECT current_user()'
```
:::note
ClickHouse will look for a JWT token in (by priority):
1. `X-ClickHouse-JWT-Token` header.
2. `Authorization` header.
3. `token` request parameter. In this case, the "Bearer" prefix should not exist.
:::
