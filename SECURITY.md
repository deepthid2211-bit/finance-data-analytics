# Security Policy

## Credential Management

This project connects to external services (Snowflake, Kafka, Yahoo Finance APIs).
Follow these rules to keep credentials safe:

1. **Never commit secrets.** The `.env` file is listed in `.gitignore` and must
   never be pushed to version control.
2. **Use `.env.example` as a template.** Copy it to `.env` and fill in your
   real credentials locally:
   ```bash
   cp .env.example .env
   ```
3. **Rotate credentials** if they are accidentally exposed. Revoke the
   compromised keys immediately and generate new ones.
4. **Limit privilege.** Use the least-privileged Snowflake role that still
   allows the pipeline to function. Avoid `ACCOUNTADMIN` in production.

## Reporting a Vulnerability

If you discover a security issue in this project, please open a private issue
or contact the maintainer directly rather than disclosing it publicly.
