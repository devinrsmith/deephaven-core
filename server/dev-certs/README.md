# Development Certificates

This are *self-signed* certificates for *local* development purposes.

Generated with the command:

```shell
openssl req -x509 -nodes -subj '/CN=localhost' -newkey rsa:1024 -keyout key.pem -out ca.crt -sha256 -days 3650
```
